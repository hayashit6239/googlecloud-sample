package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
)

type Subscriber struct {
	client         *pubsub.Client
	subscriptionID string
}

type Message struct {
	Path string `json:"path"`
}

func NewSubscriber(projectID, subscriptionID string) (*Subscriber, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %v", err)
	}

	return &Subscriber{
		client:         client,
		subscriptionID: subscriptionID,
	}, nil
}

func (s *Subscriber) Close() error {
	return s.client.Close()
}

func (s *Subscriber) Receive(ctx context.Context, handler func(ctx context.Context, msg Message) error) error {
	sub := s.client.Subscription(s.subscriptionID)
	
	return sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		var msg Message
		if err := json.Unmarshal(m.Data, &msg); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			m.Nack()
			return
		}

		if err := handler(ctx, msg); err != nil {
			log.Printf("Failed to handle message: %v", err)
			m.Nack()
			return
		}

		m.Ack()
	})
}