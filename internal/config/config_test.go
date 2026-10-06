package config

import "testing"

func TestMaxRequestBodyBytes(t *testing.T) {
	t.Setenv("MAX_REQUEST_BODY_BYTES", "")
	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxRequestBodyBytes != 32<<20 {
		t.Fatalf("default = %d, want 32 MiB", cfg.MaxRequestBodyBytes)
	}

	t.Setenv("MAX_REQUEST_BODY_BYTES", "67108864")
	cfg, err = LoadConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxRequestBodyBytes != 64<<20 {
		t.Fatalf("override = %d, want 64 MiB", cfg.MaxRequestBodyBytes)
	}

	for _, invalid := range []string{"0", "-1", "32MiB"} {
		t.Setenv("MAX_REQUEST_BODY_BYTES", invalid)
		if _, err := LoadConfigFromEnv(); err == nil {
			t.Fatalf("expected %q to be refused", invalid)
		}
	}
}
