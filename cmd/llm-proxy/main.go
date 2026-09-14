package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	agentsv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/agents/v1"
	authorizationv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/authorization/v1"
	llmv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/llm/v1"
	meteringv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/metering/v1"
	notificationsv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/notifications/v1"
	usersv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/users/v1"
	zitimgmtv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/ziti_management/v1"
	"github.com/agynio/llm-proxy/internal/apitokenresolver"
	"github.com/agynio/llm-proxy/internal/auth"
	"github.com/agynio/llm-proxy/internal/config"
	"github.com/agynio/llm-proxy/internal/grpcclient"
	"github.com/agynio/llm-proxy/internal/native"
	"github.com/agynio/llm-proxy/internal/proxy"
	"github.com/agynio/llm-proxy/internal/ziticonn"
	"github.com/agynio/llm-proxy/internal/zitimanager"
	"github.com/agynio/llm-proxy/internal/zitimgmtclient"
	"github.com/openziti/sdk-golang/ziti"
	"google.golang.org/grpc"
)

const (
	shutdownTimeout = 10 * time.Second
	// Short-lived leaves, cached per hostname. The vendor set is closed, so the
	// cache never holds more than a handful.
	leafCertificateTTL       = time.Hour
	leafCertificateCacheSize = 64
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("llm-proxy: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadConfigFromEnv()
	if err != nil {
		return err
	}

	cleanup := make([]func(), 0, 4)
	defer func() {
		for _, closeFn := range cleanup {
			closeFn()
		}
	}()

	llmClient := mustClient(cfg.LLMServiceAddress, "llm", llmv1.NewLLMServiceClient, &cleanup)
	authzClient := mustClient(cfg.AuthorizationServiceAddress, "authorization", authorizationv1.NewAuthorizationServiceClient, &cleanup)
	usersClient := mustClient(cfg.UsersServiceAddress, "users", usersv1.NewUsersServiceClient, &cleanup)
	meteringClient := mustClient(cfg.MeteringServiceAddress, "metering", meteringv1.NewMeteringServiceClient, &cleanup)
	agentsClient := mustClient(cfg.AgentsServiceAddress, "agents", agentsv1.NewAgentsServiceClient, &cleanup)
	notificationsClient := mustClient(cfg.NotificationsAddress, "notifications", notificationsv1.NewNotificationsServiceClient, &cleanup)

	apiTokenResolver := apitokenresolver.NewResolver(usersClient)

	var zitiMgmtClient *zitimgmtclient.Client
	if cfg.ZitiEnabled {
		zitiMgmtClient, err = zitimgmtclient.NewClient(cfg.ZitiManagementAddress)
		if err != nil {
			return fmt.Errorf("create ziti management client: %w", err)
		}
		defer func() {
			if closeErr := zitiMgmtClient.Close(); closeErr != nil {
				log.Printf("failed to close ziti management client: %v", closeErr)
			}
		}()
	}

	var zitiResolver auth.IdentityResolver
	if zitiMgmtClient != nil {
		zitiResolver = zitiMgmtClient
	}

	proxyHandler := proxy.NewHandler(llmClient, authzClient, meteringClient, agentsClient, &http.Client{})
	handler := auth.Middleware(zitiResolver, apiTokenResolver)(proxyHandler)

	connContext := func(ctx context.Context, conn net.Conn) context.Context {
		sourceIdentity, ok := ziticonn.SourceIdentityFromConn(conn)
		if !ok {
			return ctx
		}
		return ziticonn.WithSourceIdentity(ctx, sourceIdentity)
	}

	server := &http.Server{
		Addr:        cfg.ListenAddress,
		Handler:     handler,
		ConnContext: connContext,
	}

	errCh := make(chan error, 2)

	go func() {
		log.Printf("llm-proxy listening on %s", cfg.ListenAddress)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server stopped: %w", err)
		}
	}()

	if cfg.ZitiEnabled {
		// Native mode terminates TLS for the vendor hostnames with leaves minted
		// from the Egress CA. Absent, the platform path still serves: an
		// environment in native mode simply has nothing to carry its traffic.
		var nativeServer *native.Server
		nativeCA, err := native.LoadCertificateAuthority(cfg.EgressCACertPath, cfg.EgressCAKeyPath)
		if err != nil {
			log.Printf("llm-proxy: native mode disabled: %v", err)
		}

		var listenerMu sync.Mutex
		var currentListener net.Listener
		var currentNative *native.Listener
		listenerFactory := func(zitiCtx ziti.Context) (net.Listener, error) {
			if nativeCA != nil {
				// Rebuilt on re-enrollment alongside the platform listener: the
				// old ziti context's binds do not survive it.
				listenerMu.Lock()
				previousNative := currentNative
				vendorListener := native.NewListener(zitiCtx)
				currentNative = vendorListener
				listenerMu.Unlock()
				if previousNative != nil {
					_ = previousNative.Close()
				}

				forwarder := proxy.NewNativeForwarder(&http.Client{}, meteringClient)
				if cfg.NativeRequestDiagnostics {
					forwarder = proxy.NewDiagnosticNativeForwarder(&http.Client{}, meteringClient)
				}
				nativeServer = native.NewServer(
					vendorListener,
					zitiMgmtClient,
					llmClient,
					native.NewLeafCertificateCache(nativeCA, leafCertificateTTL, leafCertificateCacheSize, native.SystemClock()),
					forwarder,
				)
				go func(server *native.Server) {
					if err := server.Serve(ctx); err != nil && ctx.Err() == nil {
						errCh <- fmt.Errorf("native server stopped: %w", err)
					}
				}(nativeServer)
				go func(server *native.Server) {
					subscriber := native.NewInvalidationSubscriber(notificationsClient, server)
					if err := subscriber.Run(ctx); err != nil && ctx.Err() == nil {
						log.Printf("llm-proxy: invalidation subscriber stopped: %v", err)
					}
				}(nativeServer)
			}
			return zitiCtx.ListenWithOptions("llm-proxy", ziti.DefaultListenOptions())
		}
		onNewListener := func(listener net.Listener) {
			listenerMu.Lock()
			previousListener := currentListener
			currentListener = listener
			listenerMu.Unlock()

			log.Printf("llm-proxy listening on ziti service llm-proxy")
			go func(activeListener net.Listener) {
				if err := server.Serve(activeListener); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
					errCh <- fmt.Errorf("ziti server stopped: %w", err)
				}
			}(listener)

			if previousListener != nil {
				if err := previousListener.Close(); err != nil {
					log.Printf("failed to close previous ziti listener: %v", err)
				}
			}
		}

		zitiManager, err := zitimanager.New(
			ctx,
			zitiMgmtClient,
			zitimgmtv1.ServiceType_SERVICE_TYPE_LLM_PROXY,
			cfg.ZitiLeaseRenewalInterval,
			cfg.ZitiEnrollmentTimeout,
			listenerFactory,
			onNewListener,
		)
		if err != nil {
			return fmt.Errorf("setup ziti manager: %w", err)
		}
		defer zitiManager.Close()

		go func() {
			if err := zitiManager.RunLeaseRenewal(ctx); err != nil {
				log.Fatalf("terminating: %v", err)
			}
		}()
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown http: %w", err)
	}

	return nil
}

func mustClient[T any](target, name string, factory func(grpc.ClientConnInterface) T, cleanup *[]func()) T {
	client, err := grpcclient.New(target, factory)
	if err != nil {
		log.Fatalf("failed to create %s gRPC client: %v", name, err)
	}

	if cleanup != nil {
		*cleanup = append(*cleanup, func() {
			if err := client.Close(); err != nil {
				log.Printf("failed to close %s gRPC client: %v", name, err)
			}
		})
	}

	return client.Service()
}
