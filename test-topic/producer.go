package main
import (
	"context"
	"fmt"
	"log"
	"time"
	"github.com/segmentio/kafka-go"
)

func main(){
	//Define kafka writer
	//Kafka writer is used to send messages to Kafka topic
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers : []string{"localhost:9092"},
		Topic : "test-topic",
		Balancer : &kafka.LeastBytes{},
	})

	defer writer.Close()	

	//Send 10 messages
	for i := 0; i < 10; i++ {
		msg := kafka.Message{
			Key : []byte(fmt.Sprintf("Key-%d", i)),
			Value : []byte(fmt.Sprintf("Hello Kafka %d", i)),
		}

		err :=writer.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Fatalf("Failed to write messages %d: %v", i, err)
		}
		fmt.Printf("Message %d sent\n", i)
		time.Sleep(1 * time.Second)
	}
}