package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/resmoio/kubernetes-event-exporter/pkg/exporter"
	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
	"github.com/resmoio/kubernetes-event-exporter/pkg/metrics"
	"github.com/resmoio/kubernetes-event-exporter/pkg/setup"

	"github.com/phuslu/log"
)

var (
	conf       = flag.String("conf", "config.yaml", "The config path file")
	addr       = flag.String("metrics-address", ":2112", "The address to listen on for HTTP requests.")
	kubeconfig = flag.String("kubeconfig", "", "Path to the kubeconfig file to use.")
	tlsConf    = flag.String("metrics-tls-config", "", "The TLS config file for your metrics.")
)

var levels = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

func main() {
	flag.Parse()

	log.Info().Msg("Reading config file " + *conf)
	configBytes, err := os.ReadFile(*conf)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot read config file")
	}

	configBytes = []byte(os.ExpandEnv(string(configBytes)))

	cfg, err := setup.ParseConfigFromBytes(configBytes)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	if cfg.LogFormat == "json" {
		slog.SetDefault((&log.Logger{
			Level: log.InfoLevel,
		}).Slog())
		// Defaults to JSON already nothing to do
	} else if cfg.LogFormat == "" || cfg.LogFormat == "pretty" {
		slog.SetDefault((&log.Logger{
			Level: log.InfoLevel,
			Writer: &log.ConsoleWriter{
				ColorOutput: true,
			},
		}).Slog())
	} else {
		log.Fatal().Str("log_format", cfg.LogFormat).Msg("Unknown log format")
	}

	if cfg.LogLevel != "" {
		ll, ok := levels[cfg.LogLevel]
		if !ok {
			log.Fatal().Err(err).Str("level", cfg.LogLevel).Msg("Invalid log level")
		}
		slog.SetLogLoggerLevel(ll)
	} else {
		slog.Info("Set default log level to info. Use config.logLevel=[debug | info | warn | error] to overwrite.")
	}

	cfg.SetDefaults()

	slog.Info(fmt.Sprintf("Starting with config: %#v", cfg))

	if err := cfg.Validate(); err != nil {
		log.Fatal().Err(err).Msg("config validation failed")
	}

	kubecfg, err := kube.GetKubernetesConfig(*kubeconfig)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot get kubeconfig")
	}
	kubecfg.QPS = cfg.KubeQPS
	kubecfg.Burst = cfg.KubeBurst

	metrics.Init(*addr, *tlsConf)
	metricsStore := metrics.NewMetricsStore(cfg.MetricsNamePrefix)

	engine, err := exporter.NewEngine(&cfg, &exporter.ChannelBasedReceiverRegistry{MetricsStore: metricsStore})
	if err != nil {
		log.Fatal().Err(err).Msg(err.Error())
	}
	onEvent := engine.OnEvent
	if len(cfg.ClusterName) != 0 {
		onEvent = func(event *kube.EnhancedEvent) {
			// note that per code this value is not set anywhere on the kubernetes side
			// https://github.com/kubernetes/apimachinery/blob/v0.22.4/pkg/apis/meta/v1/types.go#L276
			event.ClusterName = cfg.ClusterName
			engine.OnEvent(event)
		}
	}

	w := kube.NewEventWatcher(kubecfg, cfg.Namespace, cfg.MaxEventAgeSeconds, metricsStore, onEvent, cfg.OmitLookup, cfg.CacheSize)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if cfg.LeaderElection.Enabled {
		var wasLeader bool
		slog.Info("leader election enabled")

		onStoppedLeading := func(ctx context.Context) {
			select {
			case <-ctx.Done():
				slog.Info("Context was cancelled, stopping leader election loop")
			default:
				slog.Info("Lost the leader lease, stopping leader election loop")
			}
		}

		l, err := kube.NewLeaderElector(cfg.LeaderElection.LeaderElectionID, kubecfg,
			// this method gets called when this instance becomes the leader
			func(_ context.Context) {
				wasLeader = true
				slog.Info("leader election won")
				w.Start()
			},
			// this method gets called when the leader election loop is closed
			// either due to context cancellation or due to losing the leader lease
			func() {
				onStoppedLeading(ctx)
			},
			func(identity string) {
				slog.Info("new leader observed: " + identity)
			},
		)
		if err != nil {
			log.Fatal().Err(err).Msg("create leaderelector failed")
		}

		// Run returns if either the context is canceled or client stopped holding the leader lease
		l.Run(ctx)

		// We get here either because we lost the leader lease or the context was canceled.
		// In either case we want to stop the event watcher and exit.
		// However, if we were the leader, we wait leaseDuration seconds before stopping
		// so that we don't lose events until the next leader is elected. The new leader
		// will only be elected after leaseDuration seconds.
		if wasLeader {
			slog.Info(fmt.Sprintf("waiting leaseDuration seconds before stopping: %s", kube.GetLeaseDuration()))
			time.Sleep(kube.GetLeaseDuration())
		}
	} else {
		slog.Info("leader election disabled")
		w.Start()
		<-ctx.Done()
	}

	slog.Info("Received signal to exit. Stopping.")
	w.Stop()
	engine.Stop()
}
