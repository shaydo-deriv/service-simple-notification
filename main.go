package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {

		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Header("Access-Control-Allow-Methods", "POST,HEAD,PATCH, OPTIONS, GET, PUT")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}
func matches(regExp string, val string) bool {
	match, err := regexp.MatchString(regExp, val)
	if err != nil {
		fmt.Println(err)
		return false
	}
	return match
}

type Notification struct {
	id      uint64
	userId  uint64
	payload string
}

func redis_publishNotification(rdb *redis.Client, ctx context.Context, n Notification) {
	jsonStr, _ := json.Marshal(n)
	channel := "USER_NOTIFICATIONS::" + strconv.FormatUint(n.userId, 10)
	rdb.Publish(ctx, channel, jsonStr)
}

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

func addNotification(pgdb *pgxpool.Pool, rdb *redis.Client, ctx context.Context, n Notification) (uint64, error) {
	newId, err := db_addNotification(pgdb, ctx, n)
	if err != nil {
		return newId, err
	}
	redis_publishNotification(rdb, ctx, n)
	return newId, nil
}
func getNotifications(pgdb *pgxpool.Pool, ctx context.Context, userId uint64) ([]Notification, error) {
	return db_getNotifications(pgdb, ctx, userId)
}

func serveAPI() {
	var ctx = context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	pgdb, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Print("Failed to connect to postgresql")
		return
	}

	router := gin.Default()
	router.Use(CORSMiddleware())
	router.GET("/test", func(c *gin.Context) { c.JSON(http.StatusOK, "Test") })
	router.POST("/addNotification", func(c *gin.Context) {
		var req Notification
		err := json.NewDecoder(c.Request.Body).Decode(&req)
		if err != nil {
			c.JSON(http.StatusBadRequest, map[string]any{"error": "Invalid request"})
			return
		}
		newId, err := addNotification(pgdb, rdb, ctx, req)
		if err != nil {
			errStr := fmt.Sprintf("failed to create notification: %s", err)
			c.JSON(http.StatusInternalServerError, map[string]any{"error": errStr})
			fmt.Print(errStr)
			return
		}
		c.JSON(http.StatusOK, map[string]any{"id": newId})
	})
	router.GET("/getNotifications/:userId", func(c *gin.Context) {
		userId := c.Param("userId")
		if !matches("^[0-9]+$", userId) {
			c.JSON(http.StatusBadRequest, map[string]any{"error": "invalid userId"})
			return
		}
		userIdInt, err := strconv.ParseUint(userId, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, map[string]any{"error": "invalid userId"})
			return
		}

		notifications, err := getNotifications(pgdb, ctx, userIdInt)
		if err != nil {
			errStr := fmt.Sprintf("failed to get notifications: %s", err)
			c.JSON(http.StatusInternalServerError, map[string]any{"error": errStr})
			fmt.Print(errStr)
			return
		}
		c.JSON(http.StatusOK, notifications)
	})

	router.Run("localhost:8064")

}
func main() {
	serveAPI()
}
