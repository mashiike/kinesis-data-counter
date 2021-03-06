package kinesisdatacounter

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	gv "github.com/hashicorp/go-version"
	"github.com/itchyny/gojq"
	gc "github.com/kayac/go-config"
	"github.com/mashiike/evaluator"
)

type Config struct {
	RequiredVersion string `yaml:"required_version"`

	Counters           []*CounterConfig `yaml:"counters"`
	configFilePath     string
	versionConstraints gv.Constraints
}

type CounterConfig struct {
	ID                 string      `yaml:"id,omitempty"`
	InputStreamARN     *ARN        `yaml:"input_stream_arn,omitempty"`
	OutputStreamARN    *ARN        `yaml:"output_stream_arn,omitempty"`
	AggregateStreamArn *ARN        `yaml:"aggregate_stream_arn,omitempty"`
	TargetColumn       string      `yaml:"target_column,omitempty"`
	TargetExpr         string      `yaml:"target_expr,omitempty"`
	CounterType        CounterType `yaml:"counter_type,omitempty"`
	SipHashKeyHex      string      `yaml:"siphash_key_hex"`
	JQExpr             string      `yaml:"jq_expr"`

	transformer *gojq.Query
	evaluator   evaluator.Evaluator
}

func NewDefaultConfig() *Config {
	return &Config{
		RequiredVersion: ">=0.0.0",
	}
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

const (
	defaultSipHashKey = "0ad102230405360708090a0b0c0d0e0f"
)

func (cfg *CounterConfig) Restrict() error {
	if cfg.ID == "" {
		return errors.New("id is required")
	}
	if cfg.OutputStreamARN != nil && cfg.OutputStreamARN.IsAmbiguous() {
		return fmt.Errorf("output_stream_arn must not ambiguous arn: %s", cfg.OutputStreamARN)
	}
	if cfg.AggregateStreamArn != nil && cfg.AggregateStreamArn.IsAmbiguous() {
		return fmt.Errorf("aggregate_stream_arn must not ambiguous arn: %s", cfg.AggregateStreamArn)
	}
	if cfg.AggregateStreamArn != nil && !cfg.AggregateStreamArn.IsKinesisDataStream() {
		return fmt.Errorf("aggregate_stream_arn must kinesis data stream: %s", cfg.AggregateStreamArn)
	}
	if cfg.TargetColumn == "" && cfg.TargetExpr == "" {
		return errors.New("one of either target_column or target_expr is required")
	}
	if cfg.TargetExpr != "" {
		e, err := evaluator.New(cfg.TargetExpr)
		if err != nil {
			return fmt.Errorf("target_expr parse failed: %w", err)
		}
		cfg.evaluator = e
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
		cfg.SipHashKeyHex = defaultSipHashKey
	}
	if cfg.JQExpr != "" {
		var err error
		cfg.transformer, err = gojq.Parse(cfg.JQExpr)
		if err != nil {
			return err
		}
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

//For CLI
func NewDefaultCounterConfig() *CounterConfig {
	arn := &ARN{}
	arn.Set("*")
	return &CounterConfig{
		InputStreamARN: arn,
		ID:             "__instant__",
		CounterType:    Count,
		TargetColumn:   "*",
	}
}

func (cfg *CounterConfig) SetFlags(f *flag.FlagSet) error {
	f.Var(&cfg.CounterType, "counter-type", "set instant counter type [Only at CLI]")
	f.StringVar(&cfg.ID, "counter-id", cfg.ID, "set instant counter id [Only at CLI]")
	f.StringVar(&cfg.TargetColumn, "counter-target-column", cfg.TargetColumn, "set instant counter target column [Only at CLI]")
	f.StringVar(&cfg.JQExpr, "counter-query", cfg.JQExpr, "set instant counter output query, jq expr [Only at CLI]")
	return nil
}
