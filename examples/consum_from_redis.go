package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/tengzbiao/go-delay-queue/config"
	"github.com/tengzbiao/go-delay-queue/delay_queue"
)

func main() {
	config.Init("")
	redisPool, _ := delay_queue.NewRedisPool()

	// 从队列中阻塞获取JobId, timeout还需要参考redis-read-timeout参数
	queues := []string{"test-topic"}
	for {
		jobId, err := blockPopFromReadyQueue(redisPool, queues, 3)
		if err != nil {
			fmt.Println(err)
			return
		}
		if jobId == "" {
			continue
		}
		fmt.Println("jobId:", jobId)
	}
}

// 从队列中阻塞获取JobId
func blockPopFromReadyQueue(redisPool *redis.Pool, queues []string, timeout int) (string, error) {
	conn := redisPool.Get()
	defer conn.Close()

	var args []interface{}
	for _, queue := range queues {
		args = append(args, queue)
	}
	args = append(args, timeout)
	value, err := conn.Do("BLPOP", args...)
	if err != nil {
		return "", err
	}
	if value == nil {
		return "", nil
	}
	var valueBytes []interface{}
	valueBytes = value.([]interface{})
	if len(valueBytes) == 0 {
		return "", nil
	}
	element := string(valueBytes[1].([]byte))

	return element, nil
}
