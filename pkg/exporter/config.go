package exporter

import (
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"

	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
	"github.com/resmoio/kubernetes-event-exporter/pkg/sinks"
	"k8s.io/client-go/rest"
)

const (
	DefaultCacheSize = 1024
)

// Config allows configuration
type Config struct {
	// Route is the top route that the events will match
	// TODO: There is currently a tight coupling with route and config, but not with receiver config and sink so
	// TODO: I am not sure what to do here.
	LogLevel           string                    `yaml:"logLevel"`
	LogFormat          string                    `yaml:"logFormat"`
	ThrottlePeriod     int64                     `yaml:"throttlePeriod"`
	MaxEventAgeSeconds int64                     `yaml:"maxEventAgeSeconds"`
	ClusterName        string                    `yaml:"clusterName,omitempty"`
	Namespace          string                    `yaml:"namespace"`
	LeaderElection     kube.LeaderElectionConfig `yaml:"leaderElection"`
	Route              Route                     `yaml:"route"`
	Receivers          []sinks.ReceiverConfig    `yaml:"receivers"`
	KubeQPS            float32                   `yaml:"kubeQPS,omitempty"`
	KubeBurst          int                       `yaml:"kubeBurst,omitempty"`
	MetricsNamePrefix  string                    `yaml:"metricsNamePrefix,omitempty"`
	OmitLookup         bool                      `yaml:"omitLookup,omitempty"`
	CacheSize          int                       `yaml:"cacheSize,omitempty"`
}

func (c *Config) SetDefaults() {
	if c.CacheSize == 0 {
		c.CacheSize = DefaultCacheSize
		slog.Debug("setting config.cacheSize=1024 (default)")
	}

	if c.KubeBurst == 0 {
		c.KubeBurst = rest.DefaultBurst
		slog.Debug(fmt.Sprintf("setting config.kubeBurst=%d (default)", rest.DefaultBurst))
	}

	if c.KubeQPS == 0 {
		c.KubeQPS = rest.DefaultQPS
		slog.Debug(fmt.Sprintf("setting config.kubeQPS=%.2f (default)", rest.DefaultQPS))
	}
}

func (c *Config) Validate() error {
	if err := c.validateDefaults(); err != nil {
		return err
	}
	if err := c.validateMetricsNamePrefix(); err != nil {
		return err
	}

	// No duplicate receivers
	// Receivers individually
	// Routers recursive
	return nil
}

func (c *Config) validateDefaults() error {
	if err := c.validateMaxEventAgeSeconds(); err != nil {
		return err
	}
	return nil
}

func (c *Config) validateMaxEventAgeSeconds() error {
	if c.ThrottlePeriod == 0 && c.MaxEventAgeSeconds == 0 {
		c.MaxEventAgeSeconds = 5
		slog.Info("setting config.maxEventAgeSeconds=5 (default)")
	} else if c.ThrottlePeriod != 0 && c.MaxEventAgeSeconds != 0 {
		slog.Error("cannot set both throttlePeriod (deprecated) and MaxEventAgeSeconds")
		return errors.New("validateMaxEventAgeSeconds failed")
	} else if c.ThrottlePeriod != 0 {
		log_value := strconv.FormatInt(c.ThrottlePeriod, 10)
		slog.Info("config.maxEventAgeSeconds=" + log_value)
		slog.Warn("config.throttlePeriod is deprecated, consider using config.maxEventAgeSeconds instead")
		c.MaxEventAgeSeconds = c.ThrottlePeriod
	} else {
		log_value := strconv.FormatInt(c.MaxEventAgeSeconds, 10)
		slog.Info("config.maxEventAgeSeconds=" + log_value)
	}
	return nil
}

func (c *Config) validateMetricsNamePrefix() error {
	if c.MetricsNamePrefix != "" {
		// https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
		checkResult, err := regexp.MatchString("^[a-zA-Z][a-zA-Z0-9_:]*_$", c.MetricsNamePrefix)
		if err != nil {
			return err
		}
		if checkResult {
			slog.Info("config.metricsNamePrefix='" + c.MetricsNamePrefix + "'")
		} else {
			slog.Error("config.metricsNamePrefix should match the regex: ^[a-zA-Z][a-zA-Z0-9_:]*_$")
			return errors.New("validateMetricsNamePrefix failed")
		}
	} else {
		slog.Warn("metrics name prefix is empty, setting config.metricsNamePrefix='event_exporter_' is recommended")
	}
	return nil
}
