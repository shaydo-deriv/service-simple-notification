package main

import (
	"context"
	"encoding/json"
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

type DBs struct {
	pgdb *pgxpool.Pool
	rdb  *redis.Client
	ctx  context.Context
}

func connectDBs() DBs {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "host.docker.internal:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	pgdb, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Print("Failed to connect to postgresql")
		return DBs{}
	}
	return DBs{pgdb, rdb, ctx}
}

func serveAPI(dbs DBs) {

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
		newId, err := addNotification(dbs, req)
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

		notifications, err := getNotifications(dbs, userIdInt)
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
	dbs := connectDBs()
	go rpcWorker(dbs, 5)
	serveAPI(dbs)
}
