//go:build windows || plan9

package sinks

import (
	"context"

	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
)

type SyslogConfig struct {
	Network string `yaml:"network"`
	Address string `yaml:"address"`
	Tag     string `yaml:"tag"`
}

type SyslogSink struct {
}

func NewSyslogSink(config *SyslogConfig) (Sink, error) {
	return &SyslogSink{}, nil
}

func (w *SyslogSink) Close() {
}

func (w *SyslogSink) Send(ctx context.Context, ev *kube.EnhancedEvent) error {
	return nil
}
