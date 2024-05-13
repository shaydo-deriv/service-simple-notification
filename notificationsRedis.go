package main

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func redis_publishNotification(rdb *redis.Client, ctx context.Context, n Notification) {
	jsonStr, _ := json.Marshal(n)
	channel := "USER_NOTIFICATIONS::" + strconv.FormatUint(n.userId, 10)
	rdb.Publish(ctx, channel, jsonStr)
}
