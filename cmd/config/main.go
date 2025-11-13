package config

import (
	"os"
	"path/filepath"
	"encoding/json"
	"fmt"
	"errors"
	ctx "puls/cmd/ctx"
)

type Config struct {
	Current  string              `json:"current"`
	Contexts map[string]*ctx.Context `json:"contexts"`
}

func configPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "puls", "config.json"), nil
}

func LoadConfig() (*Config, error) {
	p, err := configPath()
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return &Config{Contexts: map[string]*ctx.Context{}}, nil
		}
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	if cfg.Contexts == nil {
		cfg.Contexts = map[string]*ctx.Context{}
	}
	return &cfg, nil
}

func SaveConfig(cfg *Config) error {
	p, err := configPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(cfg, "", "  ")
	return os.WriteFile(p, b, 0o600)
}

func MustContext(cfg *Config, nameOpt string) (*ctx.Context, error) {
	name := nameOpt
	if name == "" {
		name = cfg.Current
	}
	if name == "" {
		return nil, errors.New("context is not selected; run: puls context use <name>")
	}
	ctx := cfg.Contexts[name]
	if ctx == nil {
		return nil, fmt.Errorf("context %q not found", name)
	}
	if ctx.AdminURL == "" {
		return nil, fmt.Errorf(
			"context %q is missing admin_url; set with: puls context set --name %s --url http://host:8080/admin/v2",
			name,
			name,
		)
	}
	if ctx.Tenant == "" || ctx.Namespace == "" {
		return nil, fmt.Errorf(
			"context %q is missing tenant/namespace; set with: puls context set --name %s --tenant ... --namespace ...",
			name,
			name,
		)
	}
	if ctx.HTTPTimeoutSec <= 0 {
		ctx.HTTPTimeoutSec = 10
	}
	return ctx, nil
}