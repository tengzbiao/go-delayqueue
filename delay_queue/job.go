package delay_queue

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
	"github.com/tengzbiao/go-delayqueue/config"
	"github.com/vmihailenco/msgpack"
)

// Job 使用msgpack序列化后保存到Redis,减少内存占用
type Job struct {
	Topic    string `json:"topic" msgpack:"1" binding:"required"` // 真正投递的消费队列
	Id       string `json:"id" msgpack:"2"`                       // Job唯一标识ID，确保唯一
	Delay    int64  `json:"delay" msgpack:"3" binding:"required"` // Job需要延迟的时间, 单位：秒
	ExecTime int64  `json:"exec_time" msgpack:"4"`                // Job执行的时间, 单位：秒
	Body     string `json:"body" msgpack:"5"`                     // Job消息体
}

type jobs struct {
	pool *redis.Pool
}

func newJobs(pool *redis.Pool) *jobs {
	return &jobs{pool}
}

// 获取Job
func (j *jobs) get(key string) (*Job, error) {
	conn := j.pool.Get()
	defer conn.Close()

	var (
		data []byte
		job  Job
		err  error
	)

	key = fmt.Sprintf(config.Setting.Job.Key, key)

	if data, err = redis.Bytes(conn.Do("GET", key)); err != nil {
		return nil, err
	}

	if err = msgpack.Unmarshal(data, &job); err != nil {
		return nil, err
	}

	return &job, nil
}

// 添加Job
func (j *jobs) put(key string, job *Job) error {
	conn := j.pool.Get()
	defer conn.Close()

	key = fmt.Sprintf(config.Setting.Job.Key, key)
	value, _ := msgpack.Marshal(job)

	// 延长24小时留出错误处理时间
	if _, err := conn.Do("SETEX", key, job.Delay+24*3600, value); err != nil {
		return err
	}

	return nil
}

// 删除Job
func (j *jobs) remove(key string) error {
	conn := j.pool.Get()
	defer conn.Close()

	key = fmt.Sprintf(config.Setting.Job.Key, key)
	_, err := conn.Do("DEL", key)
	return err
}
