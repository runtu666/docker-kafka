package main

import (
	"fmt"
	"github.com/Shopify/sarama"
)

func main() {
	fmt.Println("consumer_test")

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_11_0_2

	// consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		fmt.Printf("consumer_test create consumer error %s\n", err.Error())
		return
	}

	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("chat", 0, sarama.OffsetOldest)
	if err != nil {
		fmt.Printf("try create partition_consumer error %s\n", err.Error())
		return
	}
	defer partitionConsumer.Close()

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("msg offset: %d, partition: %d, timestamp: %s, value: %s\n",
				msg.Offset, msg.Partition, msg.Timestamp.String(), string(msg.Value))
		case err := <-partitionConsumer.Errors():
			fmt.Printf("err :%s\n", err.Error())
		}
	}
}
