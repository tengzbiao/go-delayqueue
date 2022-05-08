package delay_queue

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/tengzbiao/go-delay-queue/config"
	"math"
	"time"
)

type kafkaProducer struct {
	*kafka.Producer
}

type kafkaConsumer struct {
	*kafka.Consumer
}

// NewKafkaProducer 初始化kafka Producer
func NewKafkaProducer() (*kafkaProducer, error) {
	var kafkaconf = &kafka.ConfigMap{
		"api.version.request":           "true",
		"message.max.bytes":             1000000,
		"linger.ms":                     500,
		"sticky.partitioning.linger.ms": 1000,
		"retries":                       math.MaxInt32,
		"retry.backoff.ms":              1000,
		"acks":                          "1",
	}

	kafkaconf.SetKey("bootstrap.servers", config.Setting.Kafka.BootstrapServers)

	switch config.Setting.Kafka.SecurityProtocol {
	case "PLAINTEXT":
		kafkaconf.SetKey("security.protocol", "plaintext")
	case "SASL_SSL":
		kafkaconf.SetKey("security.protocol", "sasl_ssl")
		kafkaconf.SetKey("ssl.ca.location", "./config/ca-cert.pem")
		kafkaconf.SetKey("sasl.username", config.Setting.Kafka.SaslUsername)
		kafkaconf.SetKey("sasl.password", config.Setting.Kafka.SaslPassword)
		kafkaconf.SetKey("sasl.mechanism", config.Setting.Kafka.SaslMechanism)
	case "SASL_PLAINTEXT":
		kafkaconf.SetKey("security.protocol", "sasl_plaintext")
		kafkaconf.SetKey("sasl.username", config.Setting.Kafka.SaslUsername)
		kafkaconf.SetKey("sasl.password", config.Setting.Kafka.SaslPassword)
		kafkaconf.SetKey("sasl.mechanism", config.Setting.Kafka.SaslMechanism)
	default:
		return nil, kafka.NewError(kafka.ErrUnknownProtocol, "unknown protocol", true)
	}
	producer, err := kafka.NewProducer(kafkaconf)
	if err != nil {
		return nil, err
	}
	return &kafkaProducer{producer}, nil
}

func (k *kafkaProducer) Send(topic string, timestamp time.Time, data []byte) (err error) {
	msg := &kafka.Message{
		Timestamp:      timestamp,
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          data,
	}
	err = k.Producer.Produce(msg, nil)
	if err != nil {
		return err
	}
	event := <-k.Producer.Events()
	return event.(*kafka.Message).TopicPartition.Error
}

func NewKafkaConsumer() (*kafkaConsumer, error) {
	//common arguments
	var kafkaconf = &kafka.ConfigMap{
		"enable.auto.commit":        "false",
		"api.version.request":       "true",
		"auto.offset.reset":         "latest", //earliest
		"heartbeat.interval.ms":     3000,
		"session.timeout.ms":        30000,
		"max.poll.interval.ms":      120000,
		"fetch.max.bytes":           1024000,
		"max.partition.fetch.bytes": 256000,
	}
	kafkaconf.SetKey("bootstrap.servers", config.Setting.Kafka.BootstrapServers)
	kafkaconf.SetKey("group.id", config.Setting.Kafka.GroupId)

	switch config.Setting.Kafka.SecurityProtocol {
	case "PLAINTEXT":
		kafkaconf.SetKey("security.protocol", "plaintext")
	case "SASL_SSL":
		kafkaconf.SetKey("security.protocol", "sasl_ssl")
		kafkaconf.SetKey("ssl.ca.location", "./config/ca-cert.pem")
		kafkaconf.SetKey("sasl.username", config.Setting.Kafka.SaslUsername)
		kafkaconf.SetKey("sasl.password", config.Setting.Kafka.SaslPassword)
		kafkaconf.SetKey("sasl.mechanism", config.Setting.Kafka.SaslMechanism)
	case "SASL_PLAINTEXT":
		kafkaconf.SetKey("security.protocol", "sasl_plaintext")
		kafkaconf.SetKey("sasl.username", config.Setting.Kafka.SaslUsername)
		kafkaconf.SetKey("sasl.password", config.Setting.Kafka.SaslPassword)
		kafkaconf.SetKey("sasl.mechanism", config.Setting.Kafka.SaslMechanism)
	default:
		return nil, kafka.NewError(kafka.ErrUnknownProtocol, "unknown protocol", true)
	}

	consumer, err := kafka.NewConsumer(kafkaconf)
	if err != nil {
		return nil, err
	}
	return &kafkaConsumer{consumer}, nil
}
