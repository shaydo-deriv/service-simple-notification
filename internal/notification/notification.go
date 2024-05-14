package notification

import (
	"context"
	"fmt"
)

type Store interface {
	Add(ctx context.Context, n Notification) (uint64, error)
	Get(ctx context.Context, userId uint64) ([]Notification, error)
}

type Publisher interface {
	Publish(ctx context.Context, n Notification)
}

type Service struct {
	store Store
	pub   Publisher
}

type Notification struct {
	Id      int64
	UserId  int64
	Payload string
}

func New(store Store, pub Publisher) *Service {
	return &Service{
		store: store,
		pub:   pub,
	}
}

func (s *Service) Add(ctx context.Context, n Notification) (uint64, error) {
	newId, err := s.store.Add(ctx, n)
	if err != nil {
		return newId, fmt.Errorf("failed to store: %v", err)
	}
	s.pub.Publish(ctx, n)
	return newId, nil
}

func (s *Service) Get(ctx context.Context, userId uint64) ([]Notification, error) {
	return s.store.Get(ctx, userId)
}
