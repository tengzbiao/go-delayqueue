package main

import (
	"fmt"
	"github.com/tengzbiao/go-delayqueue/config"
	"github.com/tengzbiao/go-delayqueue/delay_queue"
)

func main() {
	config.Init("")
	config.Setting.Kafka.GroupId = "test-consumer"

	consumer, _ := delay_queue.NewKafkaConsumer()

	consumer.Subscribe("test-topic", nil)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Printf("read msg: %s\n", string(msg.Value))
		fmt.Println(consumer.CommitMessage(msg))
	}
}
