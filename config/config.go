package config

import (
	"gopkg.in/ini.v1"
	"log"
	"strings"
)

var (
	Setting *Config
)

const (
	// DefaultLogFileName 默认日志名
	DefaultLogFileName = "delay-queue.log"
	// DefaultLogFilePath 默认日志目录
	DefaultLogFilePath = "./logs"
	// DefaultLogLevel 默认日志级别
	DefaultLogLevel = "debug"

	// DefaultBindAddress 监听地址
	DefaultBindAddress = "0.0.0.0:9277"
	// DefaultHttpReadTimeout http读取超时时间, 单位秒
	DefaultHttpReadTimeout = 10
	// DefaultHttpWriteTimeout http写入超时时间, 单位秒
	DefaultHttpWriteTimeout = 10

	// DefaultBucketSize redis bucket数量
	DefaultBucketSize = 10
	// DefaultBucketName redis bucket名称
	DefaultBucketName = "delay_bucket_%d"

	// DefaultKafkaTopicName kafka队列名称
	DefaultKafkaTopicName = "delay_topic_%s"
	// DefaultKafkaDurations kafka支持的延迟时间类型（逗号分隔），获取topic名字: 如5s对应topic为delay_topic_5s
	DefaultKafkaDurations = "5s,10s"
	// DefaultKafkaPartitionGroups 手动给每个消费进程进行partition分组
	DefaultKafkaPartitionGroups = "[[0,1]]"

	// DefaultRedisHost Redis连接地址
	DefaultRedisHost = "127.0.0.1:6379"
	// DefaultRedisDb Redis数据库编号
	DefaultRedisDb = 0
	// DefaultRedisPassword Redis密码
	DefaultRedisPassword = ""
	// DefaultRedisMaxIdle Redis连接池闲置连接数
	DefaultRedisMaxIdle = 20
	// DefaultRedisMaxActive Redis连接池最大激活连接数, 0为不限制
	DefaultRedisMaxActive = 100
	// DefaultRedisIdleTimeout Redis空闲连接超时时间,单位秒
	DefaultRedisIdleTimeout = 300
	// DefaultRedisConnectTimeout Redis连接超时时间,单位毫秒
	DefaultRedisConnectTimeout = 5000
	// DefaultRedisReadTimeout Redis读取超时时间, 单位毫秒
	DefaultRedisReadTimeout = 5000
	// DefaultRedisWriteTimeout Redis写入超时时间, 单位毫秒
	DefaultRedisWriteTimeout = 5000

	// DefaultJobKey job存储的key
	DefaultJobKey = "delay_job:%s"
	// DefaultJobExtendTTL job存储额外时长，单位秒
	DefaultJobExtendTTL = 24 * 3600

	// DefaultKafkaGroupId 负责消费各个延迟topic
	DefaultKafkaGroupId = "delay_queue_consumer"
	// DefaultKafkaBootstrapServers kafka broker
	DefaultKafkaBootstrapServers = "127.0.0.1:9093"
	DefaultKafkaSecurityProtocol = "PLAINTEXT"
	DefaultKafkaSaslMechanism    = ""
	DefaultKafkaSaslUsername     = ""
	DefaultKafkaSaslPassword     = ""
)

// Config 应用配置
type Config struct {
	Server     ServerConfig // http server
	QueueType  string       // queue类型 (kafka | redis)
	Redis      RedisConfig  // redis配置
	Kafka      KafkaConfig  // kafka配置
	RedisQueue RedisQueue
	KafkaQueue KafkaQueue
	Log        LogConfig
	Job        JobConfig
}

type JobConfig struct {
	Key       string // job缓存前缀
	ExtendTTL int    // job额外缓存时间
}

type ServerConfig struct {
	BindAddress  string // http server 监听地址
	ReadTimeout  int
	WriteTimeout int
}

type LogConfig struct {
	Level       string
	LogFileName string
	LogPath     string
}

type KafkaConfig struct {
	GroupId          string
	BootstrapServers string
	SecurityProtocol string
	SaslMechanism    string
	SaslUsername     string
	SaslPassword     string
}

// RedisConfig Redis配置
type RedisConfig struct {
	Host           string
	Db             int
	Password       string
	MaxIdle        int // 连接池最大空闲连接数
	MaxActive      int // 连接池最大激活连接数
	IdleTimeout    int // 空闲连接超时时间, 单位秒
	ConnectTimeout int // 连接超时, 单位毫秒
	ReadTimeout    int // 读取超时, 单位毫秒
	WriteTimeout   int // 写入超时, 单位毫秒
}

type RedisQueue struct {
	BucketSize int    // bucket数量
	BucketName string // bucket在redis中的键名
}

type KafkaQueue struct {
	Topic           string
	DelayDurations  []string
	PartitionGroups string
}

// Init 初始化配置
func Init(path string) {
	Setting = &Config{}
	if path == "" {
		Setting.initDefaultConfig()
		return
	}

	Setting.parse(path)
}

// 解析配置文件
func (config *Config) parse(path string) {
	file, err := ini.Load(path)
	if err != nil {
		log.Fatalf("无法解析配置文件#%s", err.Error())
	}

	section := file.Section("")
	config.Server.BindAddress = section.Key("server.bind_address").MustString(DefaultBindAddress)
	config.Server.ReadTimeout = section.Key("server.read_timeout").MustInt(DefaultHttpReadTimeout)
	config.Server.WriteTimeout = section.Key("server.write_timeout").MustInt(DefaultHttpWriteTimeout)

	config.Redis.Host = section.Key("redis.host").MustString(DefaultRedisHost)
	config.Redis.Db = section.Key("redis.db").MustInt(DefaultRedisDb)
	config.Redis.Password = section.Key("redis.password").MustString(DefaultRedisPassword)
	config.Redis.MaxIdle = section.Key("redis.max_idle").MustInt(DefaultRedisMaxIdle)
	config.Redis.MaxActive = section.Key("redis.max_active").MustInt(DefaultRedisMaxActive)
	config.Redis.IdleTimeout = section.Key("redis.idle_timeout").MustInt(DefaultRedisIdleTimeout)
	config.Redis.ConnectTimeout = section.Key("redis.connect_timeout").MustInt(DefaultRedisConnectTimeout)
	config.Redis.ReadTimeout = section.Key("redis.read_timeout").MustInt(DefaultRedisReadTimeout)
	config.Redis.WriteTimeout = section.Key("redis.write_timeout").MustInt(DefaultRedisWriteTimeout)

	config.RedisQueue.BucketSize = section.Key("redisqueue.bucket_size").MustInt(DefaultBucketSize)
	config.RedisQueue.BucketName = section.Key("redisqueue.bucket_name").MustString(DefaultBucketName)

	config.Kafka.GroupId = section.Key("kafka.group_id").MustString(DefaultKafkaGroupId)
	config.Kafka.BootstrapServers = section.Key("kafka.bootstrap_servers").MustString(DefaultKafkaBootstrapServers)
	config.Kafka.SecurityProtocol = section.Key("kafka.security_protocol").MustString(DefaultKafkaSecurityProtocol)
	config.Kafka.SaslMechanism = section.Key("kafka.sasl_mechanism").MustString(DefaultKafkaSaslMechanism)
	config.Kafka.SaslUsername = section.Key("kafka.sasl_username").MustString(DefaultKafkaSaslUsername)
	config.Kafka.SaslPassword = section.Key("kafka.sasl_password").MustString(DefaultKafkaSaslPassword)

	config.KafkaQueue.DelayDurations = strings.Split(section.Key("kafkaqueue.delay_durations").MustString(DefaultKafkaDurations), ",")
	config.KafkaQueue.Topic = section.Key("kafkaqueue.delay_topic").MustString(DefaultKafkaTopicName)
	config.KafkaQueue.PartitionGroups = section.Key("kafkaqueue.delay_partition_groups").MustString(DefaultKafkaPartitionGroups)

	config.Log.LogPath = section.Key("log.log_path").MustString(DefaultLogFilePath)
	config.Log.LogFileName = section.Key("log.log_name").MustString(DefaultLogFileName)
	config.Log.Level = section.Key("log.level").MustString(DefaultLogLevel)

	config.Job.ExtendTTL = section.Key("job.extent_ttl").MustInt(DefaultJobExtendTTL)
	config.Job.Key = section.Key("job.key").MustString(DefaultJobKey)
}

// 初始化默认配置
func (config *Config) initDefaultConfig() {
	config.Server.BindAddress = DefaultBindAddress
	config.Server.ReadTimeout = DefaultHttpReadTimeout
	config.Server.WriteTimeout = DefaultHttpWriteTimeout

	config.Redis.Host = DefaultRedisHost
	config.Redis.Db = DefaultRedisDb
	config.Redis.Password = DefaultRedisPassword
	config.Redis.MaxIdle = DefaultRedisMaxIdle
	config.Redis.MaxActive = DefaultRedisMaxActive
	config.Redis.ConnectTimeout = DefaultRedisConnectTimeout
	config.Redis.ReadTimeout = DefaultRedisReadTimeout
	config.Redis.WriteTimeout = DefaultRedisWriteTimeout

	config.RedisQueue.BucketSize = DefaultBucketSize
	config.RedisQueue.BucketName = DefaultBucketName

	config.KafkaQueue.DelayDurations = strings.Split(DefaultKafkaDurations, ",")
	config.KafkaQueue.Topic = DefaultKafkaTopicName
	config.KafkaQueue.PartitionGroups = DefaultKafkaPartitionGroups

	config.Kafka.GroupId = DefaultKafkaGroupId
	config.Kafka.BootstrapServers = DefaultKafkaBootstrapServers
	config.Kafka.SecurityProtocol = DefaultKafkaSecurityProtocol
	config.Kafka.SaslMechanism = DefaultKafkaSaslMechanism
	config.Kafka.SaslUsername = DefaultKafkaSaslUsername
	config.Kafka.SaslPassword = DefaultKafkaSaslPassword

	config.Log.LogPath = DefaultLogFilePath
	config.Log.LogFileName = DefaultLogFileName
	config.Log.Level = DefaultLogLevel

	config.Job.ExtendTTL = DefaultJobExtendTTL
	config.Job.Key = DefaultJobKey
}
