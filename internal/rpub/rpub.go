package rpub

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/regentmarkets/sns/internal/notification"
)

type RPub struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *RPub {
	return &RPub{
		rdb: rdb,
	}
}

func (p *RPub) Publish(ctx context.Context, n notification.Notification) {
	jsonStr, _ := json.Marshal(n)
	channel := "USER_NOTIFICATIONS::" + strconv.FormatInt(n.UserId, 10)
	p.rdb.Publish(ctx, channel, jsonStr)
}
