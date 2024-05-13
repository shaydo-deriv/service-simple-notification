package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func unmarshalJsonFromRedisStreamParam[T any](value interface{}, ret T) (T, error) {
	str, ok := value.(string)
	var emptyRet T
	if !ok {
		return emptyRet, errors.New(fmt.Sprintf("Type error"))
	}
	unescaped := strings.ReplaceAll(str, "\\\"", "\"")

	err := json.Unmarshal([]byte(unescaped), &ret)
	if err != nil {
		return emptyRet, errors.New(fmt.Sprintf("Error parsing json: %s : %s", err, str))
	}
	return ret, nil

}
func handleRPCRedisMessage(msg redis.XMessage) ([]byte, string, error) {
	rpc, ok := msg.Values["rpc"].(string)
	if !ok {
		return []byte{}, "", errors.New(fmt.Sprintf("Error parsing rpc field"))
	}
	args, err := unmarshalJsonFromRedisStreamParam(msg.Values["args"], map[string]any{})
	if err != nil {
		return []byte{}, "", errors.New(fmt.Sprintf("Error parsing args field: %s", err))
	}
	stash, err := unmarshalJsonFromRedisStreamParam(msg.Values["stash"], []string{})
	if err != nil {
		return []byte{}, "", errors.New(fmt.Sprintf("Error parsing stash field: %s", err))
	}
	deadlineStr, ok := msg.Values["deadline"].(string)
	if !ok {
		return []byte{}, "", errors.New(fmt.Sprintf("Error parsing deadline field"))
	}
	deadline, err := strconv.ParseInt(deadlineStr, 10, 64)
	if err != nil {
		return []byte{}, "", errors.New(fmt.Sprintf("Error parsing deadline field: %s", err))
	}
	who, ok := msg.Values["who"].(string)
	if !ok {
		return []byte{}, "", errors.New(fmt.Sprintf("Error parsing who field"))
	}
	message_id, ok := msg.Values["message_id"].(string)
	if !ok {
		return []byte{}, "", errors.New(fmt.Sprintf("Error parsing message_id field"))
	}

	//	messageId:=msg.Values["message_id"]  //TODO - what do I do with this?
	//	req_log_context:=msg.Values["req_log_context"]  //TODO - what do I do with this?

	if time.Now().Unix() > deadline { //Deadline reached, skip processing
		fmt.Printf("Deadline reached for %s (%ss old)\n", rpc, strconv.FormatInt(time.Now().Unix()-deadline, 10))
		return []byte{}, "", nil //This doesn't return an error, so that the caller skips this and calls XAck
	}
	rpcResponse, err := handleRPC(rpc, args, stash)
	if err != nil {
		fmt.Printf("Failed to handle rpc: %s: %s\n", rpc, err)
		return []byte{}, "", err
	}
	response := map[string]any{
		"rpc":        rpc,
		"message_id": message_id,
		"response": map[string]any{
			"result":    rpcResponse,
			"timestamp": time.Now().UnixMilli() / 1000,
		},
		"deadline": deadlineStr,
		"args":     args,
		"who":      who,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		fmt.Printf("Failed to marshal rpcresponse : %s: %s\n", rpc, err)
		return []byte{}, "", err
	}

	return responseBytes, who, nil
}

func rpcWorker() {
	redisRpc := redis.NewClient(&redis.Options{
		Addr:     "host.docker.internal:6309",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	streamName := "notifications"
	var ctx = context.Background()
	consumerGroupName := "rpcWorker"
	status := redisRpc.XGroupCreate(ctx, streamName, consumerGroupName, "0")
	if status.Val() == "OK" {
		fmt.Printf("XGroupCreate returned: OK - this happens for a brand new category\n")
	} else if status.Val() != "" {
		fmt.Printf("XGroupCreate returned unexpected value: %s\n", status)
	}
	consumerName := uuid.NewString() //TODO: what should we use here is a GUID sensible?
	for {
		streamData, err := redisRpc.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    consumerGroupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    10,
			NoAck:    true, //This means we need to call XAck
		}).Result()
		if err != nil {
			fmt.Printf("Error in XReadGroup: %s\n", err)
		}
		for _, streamMessages := range streamData {
			for _, msg := range streamMessages.Messages {
				//{1715602477318-0 map[args:{"valid_source":"1","args":{"get_notifications":1,"req_id":2,"subscribe":1},"token":"a1-5tSSSkbu3gYzAyOBKDU1cXH35r0lo","logging":{},"country_code":"de","account_tokens":{"CR90000000":{"is_virtual":0,"token":"a1-5tSSSkbu3gYzAyOBKDU1cXH35r0lo","broker":"CR","app_id":"16303"}},"language":"EN","source_bypass_verification":0,"source":"1","brand":"deriv"} deadline:1715602507 message_id:16 req_log_context:{"correlation_id":"5bc42fa1-2b3a-4bee-b821-93e0e81196a7"} rpc:get_notifications stash:["language","country_code","token","account_tokens"] who:3D6EDAB6-1120-11EF-B778-0B46AC30129B]}

				fmt.Printf("Got RPC message for %s: %s\n", streamMessages.Stream, msg)
				responseBytes, who, err := handleRPCRedisMessage(msg)
				if err != nil {
					fmt.Printf("Error handling RPC: %s\n", err)
				} else {
					if who != "" {
						fmt.Printf("Sending response for RPC to %s", who)
						redisRpc.Publish(ctx, who, responseBytes)
					}
					redisRpc.XAck(ctx, streamMessages.Stream, consumerGroupName, msg.ID)
				}
			}
		}
	}

}
