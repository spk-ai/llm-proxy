package config

import "testing"

func TestNativeRequestDiagnosticsConfig(t *testing.T) {
	t.Setenv("ZITI_ENABLED", "false")
	t.Setenv("ZITI_LEASE_RENEWAL_INTERVAL", "")
	t.Setenv("ZITI_ENROLLMENT_TIMEOUT", "")
	for _, value := range []string{"", "false", "true", "invalid"} {
		t.Run("value="+value, func(t *testing.T) {
			t.Setenv("NATIVE_REQUEST_DIAGNOSTICS", value)
			cfg, err := LoadConfigFromEnv()
			if value == "invalid" {
				if err == nil {
					t.Fatal("invalid flag accepted")
				}
				return
			}
			if err != nil || cfg.NativeRequestDiagnostics != (value == "true") {
				t.Fatalf("unexpected configuration: %v", err)
			}
		})
	}
}
