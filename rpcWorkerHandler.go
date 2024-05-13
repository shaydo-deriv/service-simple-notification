package main

import (
	"errors"
	"fmt"
)

func handleRPC(dbs DBs, rpc string, args map[string]any, stash []string) (any, error) {
	if rpc == "get_notifications" {
		return handleGetNotifications(dbs, args)
	}
	return "", errors.New(fmt.Sprintf("Unknown rpc: %s", rpc))
}

func handleGetNotifications(dbs DBs, args map[string]any) (any, error) {
	userId, err := getUserIdForRPC(args)
	if err != nil {
		return "", errors.New(fmt.Sprintf("failed to get userId for notifications: %s", err))
	}
	notifications, err := getNotifications(dbs, userId)
	if err != nil {
		return "", errors.New(fmt.Sprintf("failed to get notifications: %s", err))
	}
	ret := make([]map[string]any, 0)
	for _, n := range notifications {
		ret = append(ret, map[string]any{
			"id":      n.id,
			"payload": n.payload,
		})
	}
	return ret, nil
}

func getUserIdForRPC(args map[string]any) (uint64, error) {
	return 123, nil //TODO - we need to get the websocket layer to include the userId in the
}
