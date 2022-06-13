package delay_queue

import (
	"errors"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/tengzbiao/go-delayqueue/config"
	"github.com/tengzbiao/go-delayqueue/logger"
)

// redis延迟队列，分桶实现快存快读
type redisQueue struct {
	pool         *redis.Pool
	bucket       *Bucket
	bucketNameCh <-chan string
	realQueue    string
	jobs         *jobs
}

func NewRedisQueue() (*redisQueue, error) {
	pool, _ := NewRedisPool()
	q := &redisQueue{
		pool:         pool,
		bucketNameCh: newBucketNameCh(),
		bucket:       newBucket(pool),
		jobs:         newJobs(pool),
	}

	return q, nil
}

func (r *redisQueue) Close() {
	_ = r.pool.Close()
}

// Schedule 初始化bucket调度器
func (r *redisQueue) Schedule(stop <-chan struct{}) error {
	for i := 1; i <= config.Setting.RedisQueue.BucketSize; i++ {
		ticker := time.NewTicker(time.Second)
		bucketName := fmt.Sprintf(config.Setting.RedisQueue.BucketName, i)
		logger.Infof("redis queue bucket init：%s ", bucketName)
		go r.scheduleByBucket(ticker, bucketName)
	}
	<-stop
	logger.Infof("Consumer Server Has Been Stopped")
	return nil
}

func (r *redisQueue) lock(bucketName string) bool {
	conn := r.pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("lock:%s", bucketName)

	ret, _ := redis.String(conn.Do("SET", key, 1, "NX", "EX", 1))
	return ret != ""
}

func (r *redisQueue) unlock(bucketName string) {
	conn := r.pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("lock:%s", bucketName)

	_, _ = conn.Do("DEL", key)
}

func (r *redisQueue) scheduleByBucket(ticker *time.Ticker, bucketName string) {
	for {
		select {
		case t := <-ticker.C:
			// 一个bucket只有被一个进程扫描
			if ok := r.lock(bucketName); !ok {
				break
			}
			r.tickHandler(t, bucketName)
			r.unlock(bucketName)
		}
	}
}

func (r *redisQueue) tickHandler(t time.Time, bucketName string) {
	for {
		bucketItem, err := r.bucket.Get(bucketName)
		if err != nil {
			logger.Errorf("扫描bucket错误(%v),bucketName(%s)", err, bucketName)
			return
		}

		// 集合为空
		if bucketItem == nil {
			return
		}

		// 延迟时间未到
		if bucketItem.timestamp > t.Unix() {
			logger.Debugf("job not ready, paused:%s, msgTime:%v", bucketItem.jobId, bucketItem.timestamp)
			return
		}

		// 延迟时间小于等于当前时间, 取出Job元信息并放入ready queue
		job, err := r.jobs.get(bucketItem.jobId)
		if err != nil {
			logger.Errorf("获取Job元信息失败(%v),bucketName(%s)", err, bucketName)
			return
		}

		// job元信息不存在, 从bucket中删除
		if job == nil {
			_ = r.bucket.Remove(bucketName, bucketItem.jobId)
			return
		}

		// 再次确认元信息中delay是否小于等于当前时间
		if job.ExecTime > t.Unix() {
			_ = r.bucket.ReAdd(bucketName, <-r.bucketNameCh, job.ExecTime, job.Id)
			return
		}

		logger.Debugf("job has ready:%+v, msgTime:%v", job, job.Delay)

		err = r.deliveryJob(job.Topic, bucketItem.jobId)
		if err != nil {
			logger.Errorf("job投递ready队列失败(%v), 重新进入delay队列, job(%+v), bucketName(%s)", err, job, bucketName)
			return
		}

		// 从bucket中删除
		_ = r.bucket.Remove(bucketName, bucketItem.jobId)
	}
}

// deliveryJob 投递到真实队列
func (r *redisQueue) deliveryJob(queueName, jobId string) error {
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("RPUSH", queueName, jobId)

	return err
}

// AddJob 新增job
func (r *redisQueue) AddJob(job *Job) error {
	if job.Id == "" || job.Topic == "" || job.Delay < 0 {
		return errors.New("invalid job")
	}

	job.ExecTime = time.Now().Unix() + job.Delay

	if err := r.jobs.put(job.Id, job); err != nil {
		return err
	}

	return r.bucket.Add(<-r.bucketNameCh, job.ExecTime, job.Id)
}

// RemoveJob 移除job
func (r *redisQueue) RemoveJob(jobId string) error {
	return r.jobs.remove(jobId)
}

// GetJob 获取job
func (r *redisQueue) GetJob(jobId string) (*Job, error) {
	return r.jobs.get(jobId)
}
