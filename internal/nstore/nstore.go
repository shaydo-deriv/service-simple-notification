package nstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/regentmarkets/sns/internal/notification"
)

type NStore struct {
	pg *pgxpool.Pool
}

func New(pg *pgxpool.Pool) *NStore {
	return &NStore{
		pg: pg,
	}
}

func (ns *NStore) Add(ctx context.Context, n notification.Notification) (uint64, error) {
	rows, err := ns.pg.Query(ctx, "INSERT INTO notifications (user_id,payload,created_at) VALUES($1,$2,CURRENT_TIMESTAMP()) RETURNING id", n.UserId, n.Payload)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Error adding notifications to DB: %s", err))
	}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return 0, errors.New("Error inserting notification into DB")
		} else if values[0].(int64) != -1 {
			return 0, errors.New("Error inserting notification into DB")
		}
		return values[0].(uint64), nil
	}
	return 0, errors.New("Error inserting notification into DB")
}
func (ns *NStore) Get(ctx context.Context, userId uint64) ([]notification.Notification, error) {
	rows, err := ns.pg.Query(ctx, "SELECT id,user_id,payload FROM notifications WHERE user_id=$1", userId)
	ret := []notification.Notification{}
	if err != nil {
		return ret, errors.New(fmt.Sprintf("Error getting notifications from DB: %s", err))
	}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return ret, errors.New("Error getting notifications values from DB")
		} else {
			ret = append(ret, notification.Notification{
				Id:      values[0].(int64),
				UserId:  values[1].(int64),
				Payload: values[2].(string),
			})
		}
	}
	return ret, nil
}
