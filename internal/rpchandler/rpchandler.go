package rpchandler

import (
	"context"
	"errors"
	"fmt"

	"github.com/regentmarkets/sns/internal/notification"
	"github.com/regentmarkets/sns/pkg/derivrpc"
)

type RPCHandler struct {
	nsrv *notification.Service
}

func New(nsrv *notification.Service) *RPCHandler {
	return &RPCHandler{
		nsrv: nsrv,
	}
}

func (h *RPCHandler) HandlerFunc() derivrpc.Handler {
	return func(ctx context.Context, rpc string, args map[string]any, stash []string) (any, error) {
		if rpc == "get_notifications" {
			userId, err := getUserIdForRPC(args)
			if err != nil {
				return "", errors.New(fmt.Sprintf("failed to get userId for notifications: %s", err))
			}
			notifications, err := h.nsrv.Get(ctx, userId)
			if err != nil {
				return "", errors.New(fmt.Sprintf("failed to get notifications: %s", err))
			}
			ret := make([]map[string]any, 0)
			for _, n := range notifications {
				ret = append(ret, map[string]any{
					"id":      n.Id,
					"payload": n.Payload,
				})
			}
			return ret, nil
		}
		return "", errors.New(fmt.Sprintf("Unknown rpc: %s", rpc))
	}
}

func getUserIdForRPC(args map[string]any) (uint64, error) {
	return 123, nil //TODO - we need to get the websocket layer to include the userId in the
}
