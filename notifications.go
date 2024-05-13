package main

type Notification struct {
	id      int64
	userId  int64
	payload string
}

func addNotification(dbs DBs, n Notification) (uint64, error) {
	newId, err := db_addNotification(dbs, n)
	if err != nil {
		return newId, err
	}
	redis_publishNotification(dbs, n)
	return newId, nil
}
func getNotifications(dbs DBs, userId uint64) ([]Notification, error) {
	return db_getNotifications(dbs, userId)
}
