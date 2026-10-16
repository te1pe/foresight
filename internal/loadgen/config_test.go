package loadgen

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	env := map[string]string{
		"FORESIGHT_LOADGEN_TARGETS":  "250",
		"FORESIGHT_LOADGEN_DURATION": "90s",
		"FORESIGHT_LOADGEN_LOGS_RPS": "1000",
	}
	cfg, err := LoadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Targets != 250 || cfg.Duration != 90*time.Second || cfg.LogsPerSecond != 1000 || cfg.Warmup != 20*time.Second {
		t.Errorf("cfg = %+v", cfg)
	}
	if names := targetNames(cfg); len(names) != 250 || names[7] != "http://127.0.0.1:9300/t/7/metrics" {
		t.Errorf("target names: %d, e.g. %q", len(names), names[7])
	}

	for k, v := range map[string]string{
		"FORESIGHT_LOADGEN_TARGETS":  "0",
		"FORESIGHT_LOADGEN_DURATION": "forever",
		"FORESIGHT_LOADGEN_LOGS_RPS": "-1",
	} {
		_, err := LoadConfig(func(key string) string {
			if key == k {
				return v
			}
			return ""
		})
		if err == nil || !strings.Contains(err.Error(), k) {
			t.Errorf("%s=%q: err = %v", k, v, err)
		}
	}
}
