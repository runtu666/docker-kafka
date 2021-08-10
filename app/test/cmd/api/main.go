package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
)

func main() {
	fmt.Printf("producer_test\n")
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Version = sarama.V0_11_0_2

	producer, err := sarama.NewAsyncProducer([]string{"0.0.0.0:9092"}, config)
	if err != nil {
		fmt.Printf("producer_test create producer error :%s\n", err.Error())
		return
	}
	log.Println("qqwq")

	defer producer.AsyncClose()

	// send message
	msg := &sarama.ProducerMessage{
		Topic: "chat",
		Key:   sarama.StringEncoder("go_test"),
	}

	value := "this is message"
	for {
		log.Println(value)
		msg.Value = sarama.ByteEncoder(value)
		fmt.Printf("input [%s]\n", value)
		// send to chain
		producer.Input() <- msg
		select {
		case suc := <-producer.Successes():
			fmt.Printf("offset: %d,  timestamp: %s", suc.Offset, suc.Timestamp.String())
		case fail := <-producer.Errors():
			fmt.Printf("err: %s\n", fail.Err.Error())
		}
	}
}
