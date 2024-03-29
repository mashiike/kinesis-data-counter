package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/fatih/color"
	"github.com/fujiwara/logutils"
	"github.com/handlename/ssmwrap"
	kineisdatacounter "github.com/mashiike/kinesis-data-counter"
)

var (
	Version = "current"
)
var filter = &logutils.LevelFilter{
	Levels: []logutils.LogLevel{"debug", "info", "warn", "error"},
	ModifierFuncs: []logutils.ModifierFunc{
		nil,
		logutils.Color(color.FgWhite),
		logutils.Color(color.FgYellow),
		logutils.Color(color.FgRed, color.Bold),
	},
	Writer: os.Stderr,
}

func main() {
	var ssmwrapErr error
	ssmwrapPaths := os.Getenv("SSMWRAP_PATHS")
	paths := strings.Split(ssmwrapPaths, ",")
	if ssmwrapPaths != "" && len(paths) > 0 {
		ssmwrapErr = ssmwrap.Export(ssmwrap.ExportOptions{
			Paths:   paths,
			Retries: 3,
		})
	}
	var (
		config, logLevel, stream, window string
		putRecord                        bool
	)
	instantCounterConfig := kineisdatacounter.NewDefaultCounterConfig()
	flag.StringVar(&config, "config", "", "kinesis-data-counter config")
	flag.StringVar(&logLevel, "log-level", "info", "log level")
	flag.StringVar(&stream, "stream", "", "kinesis data stream name [Only at CLI]")
	flag.StringVar(&window, "window", "", "tumbling window size, max 15m [Only at CLI]")
	flag.BoolVar(&putRecord, "put record", false, "put record configured stream [Only at CLI]")
	instantCounterConfig.SetFlags(flag.CommandLine)
	flag.VisitAll(envToFlag)
	flag.Parse()
	filter.MinLevel = logutils.LogLevel(logLevel)
	log.SetOutput(filter)

	if ssmwrapErr != nil {
		log.Printf("[error] ssmwrap export: %s", ssmwrapErr)
		os.Exit(1)
	}
	cfg := kineisdatacounter.NewDefaultConfig()
	if config != "" {
		if err := cfg.Load(config); err != nil {
			log.Printf("[error] load config: %s", err)
			os.Exit(1)
		}
	} else if !isLambda() {
		log.Println("[debug] use instant counter config")
		cfg.Counters = []*kineisdatacounter.CounterConfig{instantCounterConfig}
		if err := cfg.Restrict(); err != nil {
			log.Printf("[error] instant counter config: %s", err)
			os.Exit(1)
		}
	} else {
		log.Println("[error] config is required")
		os.Exit(1)
	}
	if err := cfg.ValidateVersion(Version); err != nil {
		log.Printf("[error] %s", err)
		os.Exit(1)
	}
	app, err := kineisdatacounter.New(cfg)
	if err != nil {
		log.Printf("[error] init app: %s", err)
		os.Exit(1)
	}
	app.SetVersion(Version)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer cancel()
	if isLambda() {
		lambda.StartWithOptions(app.Handler, lambda.WithContext(ctx))
		return
	}

	// Run as CLI command
	if stream == "" {
		log.Println("[error] stream is required")
		os.Exit(1)
	}
	if window == "" {
		log.Println("[error] window is required")
		os.Exit(1)
	}
	tumblingWindow, err := time.ParseDuration(window)
	if err != nil {
		log.Printf("[error] window parse failed: %s", err)
		os.Exit(1)
	}
	if tumblingWindow >= 15*time.Minute {
		log.Println("[error] window size over 15m")
		os.Exit(1)
	}
	app.SetOutput(os.Stdout)
	app.SetIgnorePutRecord(!putRecord)
	if err := app.Run(ctx, stream, tumblingWindow); err != nil {
		if errors.Is(err, context.Canceled) {
			log.Printf("[debug] run end status: %s", err)
			return
		}
		log.Printf("[error] run failed: %s", err)
		os.Exit(1)
	}
}

func envToFlag(f *flag.Flag) {
	name := strings.ToUpper(strings.Replace(f.Name, "-", "_", -1))
	if s, ok := os.LookupEnv("KINESIS_DATA_COUNTER_" + name); ok {
		f.Value.Set(s)
	}
}

func isLambda() bool {
	return strings.HasPrefix(os.Getenv("AWS_EXECUTION_ENV"), "AWS_Lambda") ||
		os.Getenv("AWS_LAMBDA_RUNTIME_API") != ""
}
