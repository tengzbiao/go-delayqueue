package delay_queue

import (
	"fmt"
	"strconv"

	"github.com/garyburd/redigo/redis"
	"github.com/tengzbiao/go-delay-queue/config"
)

type Bucket struct {
	pool *redis.Pool
}

type BucketItem struct {
	timestamp int64
	jobId     string
}

func newBucket(pool *redis.Pool) *Bucket {
	return &Bucket{pool}
}

func newBucketNameCh() <-chan string {
	ch := make(chan string)
	go func() {
		i := 1
		for {
			ch <- fmt.Sprintf(config.Setting.RedisQueue.BucketName, i)
			if i >= config.Setting.RedisQueue.BucketSize {
				i = 1
				continue
			}
			i++
		}
	}()
	return ch
}

const (
	SCRIPT_INCR = `
redis.call('ZREM', KEYS[1], ARGV[2])
return redis.call('ZADD', KEYS[2], ARGV[1], ARGV[2])
`
)

func (b *Bucket) Add(bucketName string, timestamp int64, jobId string) error {
	conn := b.pool.Get()
	defer conn.Close()

	_, err := conn.Do("ZADD", bucketName, timestamp, jobId)
	return err
}

func (b *Bucket) ReAdd(oldBucketName, newBucketName string, timestamp int64, jobId string) error {
	if oldBucketName == newBucketName {
		return nil
	}

	conn := b.pool.Get()
	defer conn.Close()

	lua := redis.NewScript(2, SCRIPT_INCR)
	_, err := redis.Int(lua.Do(conn, oldBucketName, newBucketName, timestamp, jobId))

	return err
}

// Get 从bucket中获取延迟时间最小的JobId
func (b *Bucket) Get(bucketName string) (*BucketItem, error) {
	conn := b.pool.Get()
	defer conn.Close()

	value, err := conn.Do("ZRANGE", bucketName, 0, 0, "WITHSCORES")
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}

	var valueBytes []interface{}
	valueBytes = value.([]interface{})
	if len(valueBytes) == 0 {
		return nil, nil
	}
	timestampStr := string(valueBytes[1].([]byte))
	item := new(BucketItem)
	item.timestamp, _ = strconv.ParseInt(timestampStr, 10, 64)
	item.jobId = string(valueBytes[0].([]byte))

	return item, nil
}

// Remove 从bucket中删除JobId
func (b *Bucket) Remove(bucketName string, jobId string) error {
	conn := b.pool.Get()
	defer conn.Close()

	_, err := conn.Do("ZREM", bucketName, jobId)
	return err
}
