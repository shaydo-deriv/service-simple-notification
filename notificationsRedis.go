package main

import (
	"encoding/json"
	"strconv"
)

func redis_publishNotification(dbs DBs, n Notification) {
	jsonStr, _ := json.Marshal(n)
	channel := "USER_NOTIFICATIONS::" + strconv.FormatInt(n.userId, 10)
	dbs.rdb.Publish(dbs.ctx, channel, jsonStr)
}
