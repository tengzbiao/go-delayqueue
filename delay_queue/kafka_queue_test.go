package delay_queue

import (
	"github.com/tengzbiao/go-delay-queue/config"
	"testing"
)

func Test_delayTTL2Topic(t *testing.T) {
	config.Init("")

	var topic string

	config.Setting.KafkaQueue.DelayDurations = []string{
		"5s", "10s", "3m", "20m", "1h", "2h",
	}

	topic = delayTTL2Topic(5)
	if topic != "delay_topic_5s" {
		t.Fatalf("delayTTl: %d, convertTopic: %s, wantTopic: %s", 5, topic, "delay_topic_5s")
	}

	topic = delayTTL2Topic(8)
	if topic != "delay_topic_10s" {
		t.Fatalf("delayTTl: %d, convertTopic: %s, wantTopic: %s", 7, topic, "delay_topic_10s")
	}

	topic = delayTTL2Topic(120)
	if topic != "delay_topic_3m" {
		t.Fatalf("delayTTl: %d, convertTopic: %s, wantTopic: %s", 120, topic, "delay_topic_3m")
	}

	topic = delayTTL2Topic(60)
	println(topic)
}
