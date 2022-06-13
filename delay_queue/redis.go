package delay_queue

import (
	"github.com/garyburd/redigo/redis"
	"github.com/tengzbiao/go-delayqueue/config"

	"log"
	"time"
)

func NewRedisPool() (*redis.Pool, error) {
	pool := &redis.Pool{
		MaxIdle:     config.Setting.Redis.MaxIdle,
		MaxActive:   config.Setting.Redis.MaxActive,
		IdleTimeout: time.Duration(config.Setting.Redis.IdleTimeout) * time.Second,
		Wait:        true,
		Dial:        redisDial,
		TestOnBorrow: func(conn redis.Conn, t time.Time) error {
			_, err := conn.Do("PING")
			return err
		},
	}
	return pool, nil
}

func redisDial() (redis.Conn, error) {
	conn, err := redis.Dial(
		"tcp",
		config.Setting.Redis.Host,
		redis.DialConnectTimeout(time.Duration(config.Setting.Redis.ConnectTimeout)*time.Millisecond),
		redis.DialReadTimeout(time.Duration(config.Setting.Redis.ReadTimeout)*time.Millisecond),
		redis.DialWriteTimeout(time.Duration(config.Setting.Redis.WriteTimeout)*time.Millisecond),
	)
	if err != nil {
		log.Fatalf("连接redis失败#%s", err.Error())
		return nil, err
	}

	if config.Setting.Redis.Password != "" {
		if _, err := conn.Do("AUTH", config.Setting.Redis.Password); err != nil {
			conn.Close()
			log.Fatalf("redis认证失败#%s", err.Error())
			return nil, err
		}
	}

	_, err = conn.Do("SELECT", config.Setting.Redis.Db)
	if err != nil {
		conn.Close()
		log.Fatalf("redis选择数据库失败#%s", err.Error())
		return nil, err
	}

	return conn, nil
}
