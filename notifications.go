package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Notification struct {
	id      uint64
	userId  uint64
	payload string
}

func addNotification(pgdb *pgxpool.Pool, rdb *redis.Client, ctx context.Context, n Notification) (uint64, error) {
	newId, err := db_addNotification(pgdb, ctx, n)
	if err != nil {
		return newId, err
	}
	redis_publishNotification(rdb, ctx, n)
	return newId, nil
}
func getNotifications(pgdb *pgxpool.Pool, ctx context.Context, userId uint64) ([]Notification, error) {
	return db_getNotifications(pgdb, ctx, userId)
}
