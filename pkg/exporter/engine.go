package exporter

import (
	"errors"
	"log/slog"
	"reflect"

	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
)

// Engine is responsible for initializing the receivers from sinks
type Engine struct {
	Route    Route
	Registry ReceiverRegistry
}

func NewEngine(config *Config, registry ReceiverRegistry) (*Engine, error) {
	for _, v := range config.Receivers {
		sink, err := v.GetSink()
		if err != nil {
			return nil, errors.New("Cannot initialize sink " + v.Name)
		}

		slog.With(
			"name", v.Name,
			"type", reflect.TypeOf(sink).String(),
		).Info("Registering sink")

		registry.Register(v.Name, sink)
	}

	return &Engine{
		Route:    config.Route,
		Registry: registry,
	}, nil
}

// OnEvent does not care whether event is add or update. Prior filtering should be done in the controller/watcher
func (e *Engine) OnEvent(event *kube.EnhancedEvent) {
	e.Route.ProcessEvent(event, e.Registry)
}

// Stop stops all registered sinks
func (e *Engine) Stop() {
	slog.Info("Closing sinks")
	e.Registry.Close()
	slog.Info("All sinks closed")
}
