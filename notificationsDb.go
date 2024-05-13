package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func db_addNotification(pgdb *pgxpool.Pool, ctx context.Context, n Notification) (uint64, error) {
	rows, err := pgdb.Query(ctx, "SELECT * FROM insertNotification(?,?,?)", n.id, n.userId, n.payload)
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
func db_getNotifications(pgdb *pgxpool.Pool, ctx context.Context, userId uint64) ([]Notification, error) {
	rows, err := pgdb.Query(ctx, "SELECT id,user_id,payload FROM notifications WHERE user_id=?", userId)
	ret := []Notification{}
	if err != nil {
		return ret, errors.New(fmt.Sprintf("Error getting notifications from DB: %s", err))
	}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return ret, errors.New("Error getting notifications values from DB")
		} else {
			ret = append(ret, Notification{
				id:      values[0].(uint64),
				userId:  values[1].(uint64),
				payload: values[2].(string),
			})
		}
	}
	return ret, nil
}
