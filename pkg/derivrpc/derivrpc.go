package derivrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Handler func(ctx context.Context, rpc string, args map[string]any, stash []string) (any, error)

type Service struct {
	rdb         *redis.Client
	maxHandlers int64
	handler     Handler
}

func New(rdb *redis.Client, maxHandlers int64, handler Handler) *Service {
	return &Service{
		rdb:         rdb,
		maxHandlers: maxHandlers,
		handler:     handler,
	}
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

func unmarshalJsonFromRedisStreamParam(value interface{}, ret any) error {
	str, ok := value.(string)
	if !ok {
		return fmt.Errorf("need string, got %T", value)
	}
	unescaped := strings.ReplaceAll(str, "\\\"", "\"")

	err := json.Unmarshal([]byte(unescaped), ret)
	if err != nil {
		return fmt.Errorf("error parsing json: %s : %s", err, str)
	}
	return nil
}

func parseRPCRedisMessage(msg redis.XMessage) (*RPCMessage, error) {
	rpc, ok := msg.Values["rpc"].(string)
	if !ok {
		return nil, fmt.Errorf("error parsing rpc field")
	}
	var args map[string]any
	err := unmarshalJsonFromRedisStreamParam(msg.Values["args"], &args)
	if err != nil {
		return nil, fmt.Errorf("error parsing args field: %s", err)
	}
	var stash []string
	err = unmarshalJsonFromRedisStreamParam(msg.Values["stash"], &stash)
	if err != nil {
		return nil, fmt.Errorf("Error parsing stash field: %s", err)
	}
	deadlineStr, ok := msg.Values["deadline"].(string)
	if !ok {
		return nil, fmt.Errorf("Error parsing deadline field")
	}
	deadline, err := strconv.ParseInt(deadlineStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("Error parsing deadline field: %s", err)
	}
	who, ok := msg.Values["who"].(string)
	if !ok {
		return nil, fmt.Errorf("Error parsing who field")
	}
	messageId, ok := msg.Values["message_id"].(string)
	if !ok {
		return nil, fmt.Errorf("Error parsing message_id field")
	}

	return &RPCMessage{
		rpc:         rpc,
		args:        args,
		stash:       stash,
		deadlineStr: deadlineStr,
		deadline:    deadline,
		who:         who,
		messageId:   messageId,
	}, nil
}

func makeRPCResponseMessage(req *RPCMessage, rpcResponse any) ([]byte, error) {
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

func (s *Service) Run(ctx context.Context) error {
	streamName := "notifications"
	consumerGroupName := "rpcWorker"
	status := s.rdb.XGroupCreate(ctx, streamName, consumerGroupName, "0")
	if status.Val() == "OK" {
		slog.Info("XGroupCreate returned: OK - this happens for a brand new category")
	} else if status.Val() != "" {
		slog.Info("XGroupCreate returned unexpected value", slog.Any("result", status))
	}
	consumerName := uuid.NewString() //TODO: what should we use here is a GUID sensible?
	for {
		streamData, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
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
								s.rdb.Publish(ctx, message.who, responseBytes)
							}
							s.rdb.XAck(ctx, streamMessages.Stream, consumerGroupName, msg.ID)
						}

					}
					go s.handleRPCRequests(ctx, message)
				}
			}
		}
	}
}

func (s *Service) handleRPCRequests(ctx context.Context, message *RPCMessage) {
	if time.Now().Unix() > message.deadline { //Deadline reached, skip processing
		fmt.Printf("Deadline reached for %s (%ss old)\n", message.rpc, strconv.FormatInt(time.Now().Unix()-message.deadline, 10))
		message.callback([]byte{}, false, nil) //Not an error, but don't send any response
	} else {
		rpcResponse, err := s.handler(ctx, message.rpc, message.args, message.stash)
		message.callback(rpcResponse, true, err)
	}
}
