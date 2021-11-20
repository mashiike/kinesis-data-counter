package kinesisdatacounter

import (
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	gv "github.com/hashicorp/go-version"
	gc "github.com/kayac/go-config"
)

type Config struct {
	RequiredVersion string `yaml:"required_version"`

	Counters           []*CounterConfig `yaml:"counters"`
	configFilePath     string
	versionConstraints gv.Constraints
}

type CounterConfig struct {
	ID              string      `yaml:"id,omitempty"`
	InputStreamARN  *ARN        `yaml:"input_stream_arn,omitempty"`
	OutputStreamARN *ARN        `yaml:"output_stream_arn,omitempty"`
	TargetColumn    string      `yaml:"target_column,omitempty"`
	CounterType     CounterType `yaml:"counter_type,omitempty"`
	SipHashKeyHex   string      `yaml:"siphash_key_hex"`
}

func NewDefaultConfig() *Config {
	return &Config{}
}

func (cfg *Config) Load(path string) error {
	if err := gc.LoadWithEnv(cfg, path); err != nil {
		return err
	}
	cfg.configFilePath = filepath.Dir(path)
	return cfg.Restrict()
}

func (cfg *Config) Restrict() error {
	if cfg.RequiredVersion != "" {
		constraints, err := gv.NewConstraint(cfg.RequiredVersion)
		if err != nil {
			return fmt.Errorf("required_version has invalid format: %w", err)
		}
		cfg.versionConstraints = constraints
	}
	if len(cfg.Counters) == 0 {
		return errors.New("must configure any counter")
	}
	for i, counter := range cfg.Counters {
		if err := counter.Restrict(); err != nil {
			return fmt.Errorf("counters[%d].%w", i, err)
		}
	}
	return nil
}

func (cfg *CounterConfig) Restrict() error {
	if cfg.ID == "" {
		return errors.New("id is required")
	}
	if cfg.OutputStreamARN != nil && cfg.OutputStreamARN.IsAmbiguous() {
		return fmt.Errorf("output_stream_arn must not ambiguous arn: %s", cfg.OutputStreamARN)
	}
	if cfg.TargetColumn == "" {
		return errors.New("target_column is required")
	}
	if cfg.CounterType == 0 {
		return errors.New("counter_type is required")
	}
	if !cfg.CounterType.IsACounterType() {
		return fmt.Errorf("counter_type accept values is %s", CounterTypeValuesString())
	}
	if cfg.TargetColumn == "*" && cfg.CounterType == ApproxCountDistinct {
		return errors.New("target_column can not set *, if counter_type is approx_count_distinct")
	}
	if cfg.CounterType == ApproxCountDistinct && cfg.SipHashKeyHex == "" {
		cfg.SipHashKeyHex = "0ad102230405360708090a0b0c0d0e0f"
	}
	return nil
}

func (c *Config) ValidateVersion(version string) error {
	if c.versionConstraints == nil {
		log.Println("[warn] required_version is empty. Skip checking required_version.")
		return nil
	}
	versionParts := strings.SplitN(version, "-", 2)
	v, err := gv.NewVersion(versionParts[0])
	if err != nil {
		log.Printf("[warn]: Invalid version format \"%s\". Skip checking required_version.", version)
		// invalid version string (e.g. "current") always allowed
		return nil
	}
	if !c.versionConstraints.Check(v) {
		return fmt.Errorf("version %s does not satisfy constraints required_version: %s", version, c.versionConstraints)
	}
	return nil
}
