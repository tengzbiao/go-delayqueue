package delay_queue

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/garyburd/redigo/redis"
	"github.com/tengzbiao/go-delayqueue/config"
	"github.com/tengzbiao/go-delayqueue/logger"
)

type partitionSlice []int

func (k partitionSlice) GetKey() string {
	bt, _ := json.Marshal(k)
	return fmt.Sprintf("delay_partition_%s", string(bt))
}

// 待分配的partition集合
type assignPartition struct {
	pool       *redis.Pool
	partitions partitionSlice
}

func newAssignPartition(pool *redis.Pool) *assignPartition {
	return &assignPartition{pool: pool}
}

func (t *assignPartition) Register() (partitionSlice, error) {
	conn := t.pool.Get()
	defer conn.Close()

	var arr []partitionSlice
	if err := json.Unmarshal([]byte(config.Setting.KafkaQueue.PartitionGroups), &arr); err != nil {
		return nil, fmt.Errorf("kafka partition分组错误: %w", err)
	}

	// 寻找没被分配的
	for _, v := range arr {
		if ret, _ := redis.String(conn.Do("SET", v.GetKey(), 1, "NX", "EX", 60)); ret != "" {
			t.partitions = v
			break
		}
	}

	if len(t.partitions) == 0 {
		return nil, fmt.Errorf("消费者没匹配到kafka partition分组")
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case _ = <-ticker.C:
				conn := t.pool.Get()
				_, _ = conn.Do("SETEX", t.partitions.GetKey(), 60, 1)
				_ = conn.Close()
			}
		}
	}()

	logger.Infof("AssignPartition Register: %v", t.partitions.GetKey())

	return t.partitions, nil
}

func (t *assignPartition) UnRegister() {
	conn := t.pool.Get()
	defer conn.Close()

	logger.Infof("AssignPartition UnRegister: %v", t.partitions.GetKey())

	_, _ = conn.Do("DEL", t.partitions.GetKey())
	t.partitions = nil
}

// 暂停消费的partition集合
type pausedTopicPartition struct {
	lock     *sync.Mutex
	m        map[kafka.TopicPartition]struct{}
	consumer *kafkaConsumer
}

func newPausedTopicPartition(consumer *kafkaConsumer) *pausedTopicPartition {
	return &pausedTopicPartition{
		lock:     new(sync.Mutex),
		m:        make(map[kafka.TopicPartition]struct{}),
		consumer: consumer,
	}
}

func (p *pausedTopicPartition) Add(tp kafka.TopicPartition) {
	p.lock.Lock()
	defer p.lock.Unlock()

	(p.m)[tp] = struct{}{}
}

func (p *pausedTopicPartition) Resume() {
	p.lock.Lock()
	defer p.lock.Unlock()

	for topicPartition := range p.m {
		if err := p.consumer.Resume([]kafka.TopicPartition{topicPartition}); err != nil {
			logger.Errorf("consumer resume err: %+v (%+v)", err, topicPartition)
		} else {
			delete(p.m, topicPartition)
		}
	}
}
