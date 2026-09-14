package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultListenAddress          = ":8080"
	defaultLLMServiceAddress      = "llm:50051"
	defaultUsersServiceAddress    = "users:50051"
	defaultAgentsServiceAddress   = "agents:50051"
	defaultAuthzServiceAddress    = "authorization:50051"
	defaultMeteringServiceAddress = "metering:50051"
	defaultZitiManagementAddress  = "ziti-management:50051"
	defaultNotificationsAddress   = "notifications:50051"
	// The Egress CA, mounted from the cert-manager secret. The same CA the
	// Egress Gateway uses and the orchestrator already distributes to
	// workloads -- a second CA would mean a second trust bundle in every image
	// for no gain.
	defaultEgressCACertPath      = "/var/run/agyn/egress-ca/tls.crt"
	defaultEgressCAKeyPath       = "/var/run/agyn/egress-ca/tls.key"
	defaultZitiLeaseInterval     = 2 * time.Minute
	defaultZitiEnrollmentTimeout = 5 * time.Minute
)

type Config struct {
	ListenAddress               string
	LLMServiceAddress           string
	UsersServiceAddress         string
	AgentsServiceAddress        string
	AuthorizationServiceAddress string
	MeteringServiceAddress      string
	ZitiManagementAddress       string
	NotificationsAddress        string
	EgressCACertPath            string
	EgressCAKeyPath             string
	ZitiEnabled                 bool
	NativeRequestDiagnostics    bool
	ZitiLeaseRenewalInterval    time.Duration
	ZitiEnrollmentTimeout       time.Duration
}

func LoadConfigFromEnv() (*Config, error) {
	zitiEnabled, err := envBool("ZITI_ENABLED")
	if err != nil {
		return nil, err
	}
	nativeRequestDiagnostics, err := envBool("NATIVE_REQUEST_DIAGNOSTICS")
	if err != nil {
		return nil, err
	}

	zitiLeaseRenewalInterval, err := envDuration("ZITI_LEASE_RENEWAL_INTERVAL", defaultZitiLeaseInterval)
	if err != nil {
		return nil, err
	}
	if zitiLeaseRenewalInterval <= 0 {
		return nil, fmt.Errorf("ZITI_LEASE_RENEWAL_INTERVAL must be positive")
	}

	zitiEnrollmentTimeout, err := envDuration("ZITI_ENROLLMENT_TIMEOUT", defaultZitiEnrollmentTimeout)
	if err != nil {
		return nil, err
	}
	if zitiEnrollmentTimeout <= 0 {
		return nil, fmt.Errorf("ZITI_ENROLLMENT_TIMEOUT must be positive")
	}

	return &Config{
		ListenAddress:               envOrDefault("LISTEN_ADDRESS", defaultListenAddress),
		LLMServiceAddress:           envOrDefault("LLM_SERVICE_ADDRESS", defaultLLMServiceAddress),
		UsersServiceAddress:         envOrDefault("USERS_SERVICE_ADDRESS", defaultUsersServiceAddress),
		AgentsServiceAddress:        envOrDefault("AGENTS_SERVICE_ADDRESS", defaultAgentsServiceAddress),
		AuthorizationServiceAddress: envOrDefault("AUTHORIZATION_SERVICE_ADDRESS", defaultAuthzServiceAddress),
		MeteringServiceAddress:      envOrDefault("METERING_SERVICE_ADDRESS", defaultMeteringServiceAddress),
		ZitiManagementAddress:       envOrDefault("ZITI_MANAGEMENT_ADDRESS", defaultZitiManagementAddress),
		NotificationsAddress:        envOrDefault("NOTIFICATIONS_SERVICE_ADDRESS", defaultNotificationsAddress),
		EgressCACertPath:            envOrDefault("EGRESS_CA_CERT_PATH", defaultEgressCACertPath),
		EgressCAKeyPath:             envOrDefault("EGRESS_CA_KEY_PATH", defaultEgressCAKeyPath),
		ZitiEnabled:                 zitiEnabled,
		NativeRequestDiagnostics:    nativeRequestDiagnostics,
		ZitiLeaseRenewalInterval:    zitiLeaseRenewalInterval,
		ZitiEnrollmentTimeout:       zitiEnrollmentTimeout,
	}, nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envBool(name string) (bool, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return false, nil
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", name, err)
	}

	return parsed, nil
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", name, err)
	}

	return parsed, nil
}
