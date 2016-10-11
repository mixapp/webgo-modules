package webgo_modules

import (
	"errors"
	"github.com/IntelliQru/logger"
	"gopkg.in/redis.v4"
)

type (
	RedisConnection struct {
		Id       string
		Host     string
		Password string
		Client   *redis.Client
	}
	RedisCluster struct {
		connections map[string]RedisConnection
		logger      *logger.Logger
	}
)

var redisCluster *RedisCluster

func NewRedisCluster(log *logger.Logger) *RedisCluster {
	if redisCluster == nil {
		redisCluster = &RedisCluster{
			connections: make(map[string]RedisConnection),
			logger:      log,
		}
	}

	return redisCluster
}

func GetRedisCluster() *RedisCluster {
	return redisCluster
}

func (r *RedisCluster) NewConnection(conn RedisConnection) (client *redis.Client, err error) {

	var options = redis.Options{
		Network:  "tcp",
		Addr:     conn.Host,
		Password: conn.Password,
		DB:       0,
	}

	client = redis.NewClient(&options)

	err = client.Ping().Err()
	if err != nil {
		r.logger.Error("Error connecting redis on server "+conn.Host, err)
		return
	}

	conn.Client = client

	r.connections[conn.Id] = conn

	return
}

func (r *RedisCluster) GetConnection(id string) (client *redis.Client, err error) {

	conn, ok := r.connections[id]
	if !ok {
		err = errors.New("Connection ID not found")
		return
	}

	client = conn.Client

	return
}
