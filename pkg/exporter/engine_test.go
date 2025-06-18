package exporter

import (
	"testing"

	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
	"github.com/resmoio/kubernetes-event-exporter/pkg/sinks"
	"github.com/stretchr/testify/assert"
)

func TestEngineNoRoutes(t *testing.T) {
	cfg := &Config{
		Route:     Route{},
		Receivers: nil,
	}

	e, err := NewEngine(cfg, &SyncRegistry{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		t.FailNow()
	} else {
		ev := &kube.EnhancedEvent{}
		e.OnEvent(ev)
	}
}

func TestEngineSimple(t *testing.T) {
	config := &sinks.InMemoryConfig{}
	cfg := &Config{
		Route: Route{
			Match: []Rule{{
				Receiver: "in-mem",
			}},
		},
		Receivers: []sinks.ReceiverConfig{{
			Name:     "in-mem",
			InMemory: config,
		}},
	}

	e, err := NewEngine(cfg, &SyncRegistry{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		t.FailNow()
	} else {
		ev := &kube.EnhancedEvent{}
		e.OnEvent(ev)
		assert.Contains(t, config.Ref.Events, ev)
	}
}

func TestEngineDropSimple(t *testing.T) {
	config := &sinks.InMemoryConfig{}
	cfg := &Config{
		Route: Route{
			Drop: []Rule{{
				// Drops anything
			}},
			Match: []Rule{{
				Receiver: "in-mem",
			}},
		},
		Receivers: []sinks.ReceiverConfig{{
			Name:     "in-mem",
			InMemory: config,
		}},
	}

	e, err := NewEngine(cfg, &SyncRegistry{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		t.FailNow()
	} else {
		ev := &kube.EnhancedEvent{}
		e.OnEvent(ev)
		assert.Contains(t, config.Ref.Events, ev)
	}

	ev := &kube.EnhancedEvent{}
	e.OnEvent(ev)

	assert.NotContains(t, config.Ref.Events, ev)
	assert.Empty(t, config.Ref.Events)
}
