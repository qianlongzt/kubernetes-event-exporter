package exporter

import (
	"context"
	"log/slog"

	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
	"github.com/resmoio/kubernetes-event-exporter/pkg/sinks"
)

// SyncRegistry is for development purposes and performs poorly and blocks when an event is received so it is
// not suited for high volume & production workloads
type SyncRegistry struct {
	reg map[string]sinks.Sink
}

func (s *SyncRegistry) SendEvent(name string, event *kube.EnhancedEvent) {
	err := s.reg[name].Send(context.Background(), event)
	if err != nil {
		slog.With(
			"sink", name,
			"event", string(event.UID),
			"err", err,
		).Error("Cannot send event")
	}
}

func (s *SyncRegistry) Register(name string, sink sinks.Sink) {
	if s.reg == nil {
		s.reg = make(map[string]sinks.Sink)
	}

	s.reg[name] = sink
}

func (s *SyncRegistry) Close() {
	for name, sink := range s.reg {
		slog.With("sink", name).Info("Closing sink")
		sink.Close()
	}
}
