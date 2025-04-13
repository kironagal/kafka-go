package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"github.com/segmentio/kafka-go"
)

func main(){
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:  "test-topic",
		GroupID: "go-consumer-group-1",
		StartOffset: kafka.FirstOffset,
	})

	defer reader.Close()

	fmt.Println("Kafka consumer started...")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("could not read message: %v", err)
		}
		fmt.Printf("Received message: %s, %s, at Offset=%d\n", string(msg.Key), string(msg.Value), msg.Offset)
		time.Sleep(1 * time.Second)
	}
}