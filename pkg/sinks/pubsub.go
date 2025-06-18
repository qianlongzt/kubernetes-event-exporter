package sinks

import (
	"context"
	"fmt"
	"log/slog"

	"cloud.google.com/go/pubsub"
	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
)

type PubsubConfig struct {
	GcloudProjectId string `yaml:"gcloud_project_id"`
	Topic           string `yaml:"topic"`
	CreateTopic     bool   `yaml:"create_topic"`
}

type PubsubSink struct {
	cfg          *PubsubConfig
	pubsubClient *pubsub.Client
	topic        *pubsub.Topic
}

func NewPubsubSink(cfg *PubsubConfig) (Sink, error) {
	ctx := context.Background()
	pubsubClient, err := pubsub.NewClient(ctx, cfg.GcloudProjectId) // TODO: add options here
	if err != nil {
		return nil, err
	}

	var topic *pubsub.Topic
	if cfg.CreateTopic {
		topic, err = pubsubClient.CreateTopic(context.Background(), cfg.Topic)
		if err != nil {
			return nil, err
		}
		slog.Info(fmt.Sprintf("pubsub: created topic: %s", cfg.Topic))
	} else {
		topic = pubsubClient.Topic(cfg.Topic)
	}

	return &PubsubSink{
		pubsubClient: pubsubClient,
		topic:        topic,
		cfg:          cfg,
	}, nil
}

func (ps *PubsubSink) Send(ctx context.Context, ev *kube.EnhancedEvent) error {
	msg := &pubsub.Message{
		Data: ev.ToJSON(),
	}
	_, err := ps.topic.Publish(ctx, msg).Get(ctx)
	return err
}

func (ps *PubsubSink) Close() {
	slog.Info("pubsub: Closing topic...")
	ps.pubsubClient.Close()
}
