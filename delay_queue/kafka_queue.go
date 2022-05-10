package delay_queue

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/tengzbiao/go-delay-queue/config"
	"github.com/tengzbiao/go-delay-queue/logger"
)

type kafkaQueue struct {
	producer             *kafkaProducer
	consumer             *kafkaConsumer
	jobs                 *jobs                 // jobs
	pausedTopicPartition *pausedTopicPartition // 暂停消费的topic partition集合
	assignPartition      *assignPartition      // 待分配的partition
}

func NewKafkaQueue() (*kafkaQueue, error) {
	pool, _ := NewRedisPool()

	producer, err := NewKafkaProducer()
	if err != nil {
		return nil, fmt.Errorf("NewKafkaProducer error: %w", err)
	}
	consumer, err := NewKafkaConsumer()
	if err != nil {
		return nil, fmt.Errorf("NewKafkaConsumer error: %w", err)
	}

	c := &kafkaQueue{
		producer:             producer,
		consumer:             consumer,
		jobs:                 newJobs(pool),
		pausedTopicPartition: newPausedTopicPartition(consumer),
		assignPartition:      newAssignPartition(pool),
	}
	return c, nil
}

func (k *kafkaQueue) Close() {
	k.producer.Close()
	_ = k.consumer.Close()
}

// delayTTL转化为topic，寻找时间最相近的
func delayTTL2Topic(delayTTL int64) string {
	var (
		targetDuration = time.Duration(delayTTL) * time.Second
		durations      = config.Setting.KafkaQueue.DelayDurations
		left           = 0
		right          = len(durations) - 1
	)
	for left != right {
		midIndex := left + (right-left)/2
		midValue := durations[midIndex]
		midDuration, err := time.ParseDuration(midValue)
		if err != nil {
			right = midIndex
			continue
		}
		if targetDuration == midDuration {
			return fmt.Sprintf(config.Setting.KafkaQueue.Topic, midValue)
		}
		if targetDuration > midDuration {
			left = midIndex
		} else {
			right = midIndex
		}
		if right-left == 1 {
			break
		}
	}
	rightDuration, _ := time.ParseDuration(durations[right])
	leftDuration, _ := time.ParseDuration(durations[left])
	if math.Abs(float64(rightDuration)-float64(targetDuration)) > math.Abs(float64(leftDuration)-float64(targetDuration)) {
		return fmt.Sprintf(config.Setting.KafkaQueue.Topic, durations[left])
	}
	return fmt.Sprintf(config.Setting.KafkaQueue.Topic, durations[right])
}

// AddJob 添加job
func (k *kafkaQueue) AddJob(job *Job) error {
	if job.Id == "" || job.Topic == "" || job.Delay < 0 {
		return errors.New("invalid job")
	}

	job.ExecTime = job.Delay + time.Now().Unix()

	if err := k.jobs.put(job.Id, job); err != nil {
		return err
	}

	topic := delayTTL2Topic(job.Delay)
	return k.producer.Send(topic, time.Unix(job.ExecTime, 0), []byte(job.Id))
}

// RemoveJob 移除job
func (k *kafkaQueue) RemoveJob(jobId string) error {
	return k.jobs.remove(jobId)
}

// GetJob 获取job元信息
func (k *kafkaQueue) GetJob(jobId string) (*Job, error) {
	return k.jobs.get(jobId)
}

// Schedule 监听队列，到期投递，没到期重新入队列暂停任务执行（避免sleep导致rebalance）
func (k *kafkaQueue) Schedule(stop <-chan struct{}) error {
	// 获取要订阅的partition
	partitions, err := k.assignPartition.Register()
	if err != nil {
		return err
	}
	// 监听队列
	total := len(config.Setting.KafkaQueue.DelayDurations) * len(partitions)
	topicPartitions := make([]kafka.TopicPartition, 0, total)
	for _, duration := range config.Setting.KafkaQueue.DelayDurations {
		topic := fmt.Sprintf(config.Setting.KafkaQueue.Topic, duration)
		for _, v := range partitions {
			topicPartitions = append(topicPartitions, kafka.TopicPartition{Topic: &topic, Partition: int32(v)})
		}
	}

	logger.Infof("Assigned TopicPartitions:%+v", topicPartitions)

	if len(topicPartitions) == 0 {
		return errors.New("TopicPartitions Empty")
	}

	err = k.consumer.Assign(topicPartitions)
	if err != nil {
		return err
	}

	stopped := false

	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case _ = <-ticker.C:
				k.pausedTopicPartition.Resume()
				_, _ = k.consumer.Commit()
			case <-stop:
				k.assignPartition.UnRegister()
				stopped = true
				return
			}
		}
	}()

	for stopped == false {
		msg, err := k.consumer.ReadMessage(2 * time.Second)
		if err != nil {
			if !errors.Is(err, kafka.NewError(kafka.ErrTimedOut, "", false)) {
				logger.Errorf("Consumer ReadMessage Err:%v, msg(%v)", err, msg)
			}
			continue
		}

		var (
			job     *Job
			jobId   = string(msg.Value)
			jobTime time.Time
		)

		// 消息未到执行时间
		if msg.Timestamp.After(time.Now()) {
			logger.Debugf("job not ready, paused:%s, msgTime:%v", jobId, msg.Timestamp)
			goto Paused
		}

		// 获取job元信息
		job, err = k.jobs.get(jobId)
		if err != nil {
			logger.Errorf("job获取失败、重新进入delay队列:%v, jobId(%v)", err, jobId)
			goto Paused
		}

		if job == nil {
			continue
		}

		// 再次确认job元信息中时间是否到期
		jobTime = time.Unix(job.ExecTime, 0)
		if jobTime.After(time.Now()) {
			logger.Debugf("job not ready, paused:%s, msgTime:%v", jobId, jobTime)
			goto Paused
		}

		// 投递到真实队列
		if err = k.deliveryJob(job.Topic, jobId); err == nil {
			logger.Debugf("job has ready:%+v, msgTime:%v", job, jobTime)
			continue
		}
		logger.Errorf("job投递ready队列失败(%v), 重新进入delay队列, job(%+v)", err, job)

	Paused:
		if err = k.pauseAndSeekTopicPartition(msg.TopicPartition); err != nil {
			logger.Errorf("Consumer PauseAndSeekTopicPartition Err:%v, jobId(%s), topicPartition(%+v)", err, jobId, msg.TopicPartition)
		}
	}
	logger.Infof("Consumer Server Has Been Stopped")
	return nil
}

// pauseAndSeekTopicPartition 暂停消费并重置offset
func (k *kafkaQueue) pauseAndSeekTopicPartition(tp kafka.TopicPartition) (err error) {
	if err = k.consumer.Pause([]kafka.TopicPartition{tp}); err != nil {
		return
	}

	k.pausedTopicPartition.Add(tp)

	return k.consumer.Seek(tp, 5000)
}

// deliveryJob 投递到真实队列
func (k *kafkaQueue) deliveryJob(queueName, jobId string) error {
	return k.producer.Send(queueName, time.Now(), []byte(jobId))
}
