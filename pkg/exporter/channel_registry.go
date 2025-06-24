package exporter

import (
	"context"
	"log/slog"
	"sync"

	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
	"github.com/resmoio/kubernetes-event-exporter/pkg/metrics"
	"github.com/resmoio/kubernetes-event-exporter/pkg/sinks"
)

// ChannelBasedReceiverRegistry creates two channels for each receiver. One is for receiving events and other one is
// for breaking out of the infinite loop. Each message is passed to receivers
// This might not be the best way to implement such feature. A ring buffer can be better
// and we might need a mechanism to drop the vents
// On closing, the registry sends a signal on all exit channels, and then waits for all to complete.
type ChannelBasedReceiverRegistry struct {
	ch           map[string]chan kube.EnhancedEvent
	exitCh       map[string]chan any
	wg           *sync.WaitGroup
	MetricsStore *metrics.Store
}

func (r *ChannelBasedReceiverRegistry) SendEvent(name string, event *kube.EnhancedEvent) {
	ch := r.ch[name]
	if ch == nil {
		slog.With("name", name).Error("There is no channel")
	}

	go func() {
		ch <- *event
	}()
}

func (r *ChannelBasedReceiverRegistry) Register(name string, receiver sinks.Sink) {
	if r.ch == nil {
		r.ch = make(map[string]chan kube.EnhancedEvent)
		r.exitCh = make(map[string]chan any)
	}

	ch := make(chan kube.EnhancedEvent)
	exitCh := make(chan any)

	r.ch[name] = ch
	r.exitCh[name] = exitCh

	if r.wg == nil {
		r.wg = &sync.WaitGroup{}
	}
	r.wg.Add(1)

	go func() {
		l := slog.With("sink", name)
	Loop:
		for {
			select {
			case ev := <-ch:
				l := l.With(slog.String("event", ev.Message))
				l.Debug("sending event to sink")
				err := receiver.Send(context.Background(), &ev)
				if err != nil {
					r.MetricsStore.SendErrors.Inc()
					l.With(slog.Any("err", err)).Error("Cannot send event")
				}
			case <-exitCh:
				l.Info("Closing the sink")
				break Loop
			}
		}
		receiver.Close()
		l.Info("Closed")
		r.wg.Done()
	}()
}

// Close signals closing to all sinks and waits for them to complete.
// The wait could block indefinitely depending on the sink implementations.
func (r *ChannelBasedReceiverRegistry) Close() {
	// Send exit command and wait for exit of all sinks
	for _, ec := range r.exitCh {
		ec <- 1
	}
	r.wg.Wait()
}
