package main

import (
	"errors"
	"fmt"
)

func db_addNotification(dbs DBs, n Notification) (uint64, error) {
	rows, err := dbs.pgdb.Query(dbs.ctx, "INSERT INTO notifications (user_id,payload,created_at) VALUES($1,$2,CURRENT_TIMESTAMP()) RETURNING id", n.userId, n.payload)
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
func db_getNotifications(dbs DBs, userId uint64) ([]Notification, error) {
	rows, err := dbs.pgdb.Query(dbs.ctx, "SELECT id,user_id,payload FROM notifications WHERE user_id=$1", userId)
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
				id:      values[0].(int64),
				userId:  values[1].(int64),
				payload: values[2].(string),
			})
		}
	}
	return ret, nil
}
