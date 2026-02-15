// Package config defines the configuration structures and loading logic.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ChannelConfig configures a unidirectional transport channel.
type ChannelConfig struct {
	Mode    string `yaml:"mode"`    // "connect" or "listen"
	Address string `yaml:"address"` // host:port to connect to or listen on
}

// ServiceConfig defines a proxied service entry.
type ServiceConfig struct {
	Name   string `yaml:"name"`             // Unique service name (links inner/outer)
	Listen string `yaml:"listen,omitempty"` // Inner: address to listen on
	Target string `yaml:"target,omitempty"` // Outer: address to connect to
}

// InnerConfig is the configuration for the inner proxy (internal network side).
type InnerConfig struct {
	Services []ServiceConfig `yaml:"services"` // Services to expose
	Forward  ChannelConfig   `yaml:"forward"`  // Forward channel (inner → outer)
	Reverse  ChannelConfig   `yaml:"reverse"`  // Reverse channel (outer → inner)
}

// OuterConfig is the configuration for the outer proxy (DMZ side).
type OuterConfig struct {
	Services []ServiceConfig `yaml:"services"` // Target service mappings
	Forward  ChannelConfig   `yaml:"forward"`  // Forward channel (inner → outer)
	Reverse  ChannelConfig   `yaml:"reverse"`  // Reverse channel (outer → inner)
}

// LoadInnerConfig loads and validates an InnerConfig from a YAML file.
func LoadInnerConfig(path string) (*InnerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	var cfg InnerConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := validateInnerConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// LoadOuterConfig loads and validates an OuterConfig from a YAML file.
func LoadOuterConfig(path string) (*OuterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	var cfg OuterConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := validateOuterConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func validateInnerConfig(cfg *InnerConfig) error {
	if len(cfg.Services) == 0 {
		return fmt.Errorf("config: at least one service is required")
	}
	for i, svc := range cfg.Services {
		if svc.Name == "" {
			return fmt.Errorf("config: service[%d] missing name", i)
		}
		if svc.Listen == "" {
			return fmt.Errorf("config: service[%d] %q missing listen address", i, svc.Name)
		}
	}
	if err := validateChannel("forward", &cfg.Forward); err != nil {
		return err
	}
	return validateChannel("reverse", &cfg.Reverse)
}

func validateOuterConfig(cfg *OuterConfig) error {
	if len(cfg.Services) == 0 {
		return fmt.Errorf("config: at least one service is required")
	}
	for i, svc := range cfg.Services {
		if svc.Name == "" {
			return fmt.Errorf("config: service[%d] missing name", i)
		}
		if svc.Target == "" {
			return fmt.Errorf("config: service[%d] %q missing target address", i, svc.Name)
		}
	}
	if err := validateChannel("forward", &cfg.Forward); err != nil {
		return err
	}
	return validateChannel("reverse", &cfg.Reverse)
}

func validateChannel(name string, ch *ChannelConfig) error {
	if ch.Mode != "connect" && ch.Mode != "listen" {
		return fmt.Errorf("config: %s channel mode must be 'connect' or 'listen', got %q", name, ch.Mode)
	}
	if ch.Address == "" {
		return fmt.Errorf("config: %s channel address is required", name)
	}
	return nil
}
