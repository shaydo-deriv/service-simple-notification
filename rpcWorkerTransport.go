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

type RPCMessage struct {
	rpc         string
	args        map[string]any
	stash       []string
	deadlineStr string
	deadline    int64
	who         string
	messageId   string
	callback    func(any, bool, error) //callback function to send response
}

func parseRPCRedisMessage(msg redis.XMessage) (RPCMessage, error) {
	var invalid RPCMessage
	rpc, ok := msg.Values["rpc"].(string)
	if !ok {
		return invalid, errors.New(fmt.Sprintf("Error parsing rpc field"))
	}
	args, err := unmarshalJsonFromRedisStreamParam(msg.Values["args"], map[string]any{})
	if err != nil {
		return invalid, errors.New(fmt.Sprintf("Error parsing args field: %s", err))
	}
	stash, err := unmarshalJsonFromRedisStreamParam(msg.Values["stash"], []string{})
	if err != nil {
		return invalid, errors.New(fmt.Sprintf("Error parsing stash field: %s", err))
	}
	deadlineStr, ok := msg.Values["deadline"].(string)
	if !ok {
		return invalid, errors.New(fmt.Sprintf("Error parsing deadline field"))
	}
	deadline, err := strconv.ParseInt(deadlineStr, 10, 64)
	if err != nil {
		return invalid, errors.New(fmt.Sprintf("Error parsing deadline field: %s", err))
	}
	who, ok := msg.Values["who"].(string)
	if !ok {
		return invalid, errors.New(fmt.Sprintf("Error parsing who field"))
	}
	messageId, ok := msg.Values["message_id"].(string)
	if !ok {
		return invalid, errors.New(fmt.Sprintf("Error parsing message_id field"))
	}
	//	req_log_context:=msg.Values["req_log_context"]  //TODO - what do I do with this?

	return RPCMessage{
		rpc:         rpc,
		args:        args,
		stash:       stash,
		deadlineStr: deadlineStr,
		deadline:    deadline,
		who:         who,
		messageId:   messageId,
	}, nil
}

func makeRPCResponseMessage(req RPCMessage, rpcResponse any) ([]byte, error) {
	response := map[string]any{
		"rpc":        req.rpc,
		"message_id": req.messageId,
		"response": map[string]any{
			"result":    rpcResponse,
			"timestamp": time.Now().UnixMilli() / 1000,
		},
		"deadline": req.deadlineStr,
		"args":     req.args,
		"who":      req.who,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		fmt.Printf("Failed to marshal rpcresponse : %s: %s\n", req.rpc, err)
		return []byte{}, err
	}

	return responseBytes, nil

}

func listenToRedisForRPC(rpcChannel chan RPCMessage) {
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

				message, err := parseRPCRedisMessage(msg)
				if err != nil {
					fmt.Printf("Error handling RPC: %s\n", err)
				} else {
					message.callback = func(rpcResponse any, sendResponse bool, err error) {
						if err != nil {
							fmt.Printf("Failed to handle rpc: %s: %s\n", message.rpc, err)
						}
						responseBytes, err := makeRPCResponseMessage(message, rpcResponse)
						if err != nil {
							fmt.Printf("Error handling RPC: %s\n", err)
						} else {
							if sendResponse {
								fmt.Printf("Sending response for RPC to %s", message.who)
								redisRpc.Publish(ctx, message.who, responseBytes)
							}
							redisRpc.XAck(ctx, streamMessages.Stream, consumerGroupName, msg.ID)
						}

					}
					rpcChannel <- message

				}

			}
		}
	}
}
func handleRPCRequests(dbs DBs, rpcChannel chan RPCMessage) {
	for {
		message := <-rpcChannel
		//fmt.Printf("Handling RPC message: %s\n", message)
		if time.Now().Unix() > message.deadline { //Deadline reached, skip processing
			fmt.Printf("Deadline reached for %s (%ss old)\n", message.rpc, strconv.FormatInt(time.Now().Unix()-message.deadline, 10))
			message.callback([]byte{}, false, nil) //Not an error, but don't send any response
		} else {
			rpcResponse, err := handleRPC(dbs, message.rpc, message.args, message.stash)
			message.callback(rpcResponse, true, err)
		}
	}
}
func rpcWorker(dbs DBs, numHandlers int) {
	rpcChannel := make(chan RPCMessage, 100)
	go listenToRedisForRPC(rpcChannel)

	for ii := 0; ii < numHandlers; ii++ {
		go handleRPCRequests(dbs, rpcChannel)
	}
}
