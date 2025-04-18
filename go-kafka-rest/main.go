package main

import(
	"context"
	"log"
	"fmt"
	"net/http"
	"time"
	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"

)

var kafkaWriter *kafka.Writer

type KafkaMessage struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Offset int64  `json:"offset"`
	Topic string `json:"topic"`
	Partition int `json:"partition"`
}

func main(){
	//Set up Kafka writer
	kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers : []string{"localhost:9092"},
		Topic : "test-topic",
		Balancer : &kafka.LeastBytes{},
	})

	defer kafkaWriter.Close()

	//Set up GIN server
	router := gin.Default()

	//Route to handle produce Kafka message
	router.POST("/produce", produceHandler)
	router.GET("/consume", consumeHandler)
	router.GET("/stream", streamHandler)

	//Run API on port 8080
	log.Println("API running on localhost:8080")
	router.Run(":8080")
}

type MessageRequest struct {
	Key string `json:"Key"`
	Value string `json:"Value"`
}

func produceHandler(c *gin.Context){
	var req MessageRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error":"Invalid JSON Request"})
		return
	}

	msg := kafka.Message{
		Key: []byte(req.Key),
		Value: []byte(req.Value),
	}

	if err := kafkaWriter.WriteMessages(context.Background(), msg); err != nil {
		log.Printf("Error writing message to kafka: %v\n",err)
		c.JSON(http.StatusInternalServerError, gin.H{"error":"Kafka write failed"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message":"Message sent to Kafka", "key": req.Key})
}

func consumeHandler(c *gin.Context){
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"host.docker.internal:9092"},
		Topic: "test-topic",
		GroupID: "go-api-consumer-batch", //creating a batch consumer
		MinBytes : 1,
		MaxBytes: 10e6,
	})
	
	defer reader.Close()

	var messages []KafkaMessage
	ctx := context.Background()

	//Consume upto 10 messages or stop after 3 seconds
	timeout := time.After(3 *time.Second)
	for i := 0; i < 10; i++ {
		select {
		case <- timeout:
			goto END
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Kafka read error: %v\n", err)
				goto END
			}	

			messages = append(messages, KafkaMessage {
				Key: string(msg.Key),
				Value: string(msg.Value),
				Offset: msg.Offset,
				Topic: msg.Topic,
				Partition: msg.Partition,
			})
		}
	} 

	END:

	if len(messages) == 0 {
		c.JSON(http.StatusOK, gin.H{"message":"No messages available"})
		return
	}

	c.JSON(http.StatusOK, messages)

	//Read one message with timeout
	// ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// defer cancel()

	// msg, err := reader.ReadMessage(ctx)
	// if err != nil {
	// 	log.Printf("Kafka read timeout or error: %v\n", err)
	// 	c.JSON(http.StatusOK, gin.H{"message":"No messages available"})
	// 	return
	// }

	// c.JSON(http.StatusOK, KafkaMessage{
	// 	Key: string(msg.Key),
	// 	Value: string(msg.Value),
	// 	Offset: msg.Offset,
	// 	Topic: msg.Topic,
	// 	Partition: msg.Partition,
	// })
}

func streamHandler(c *gin.Context){
	c.Writer.Header().Set("Content-Type","text/event-stream")
	c.Writer.Header().Set("Cache-Control","no-cache")
	c.Writer.Header().Set("Connection","keep-alive")
	c.Writer.Flush()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic: "test-topic",
		GroupID: "go-stream-consumer",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	defer reader.Close()

	ctx := c.Request.Context()

	for {
		select {
		case <- ctx.Done():
			log.Println("Client disconnected")
			return
		default:
			msg, err :=reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Kafka read error: %v\n", err)
				time.Sleep(1 * time.Second) // Sleep for a second before retrying
				continue
			}
			
			data := fmt.Sprintf("data: {\"key\": \"%s\", \"value\": \"%s\",}\n\n", msg.Key, msg.Value)
			_, err = c.Writer.Write([]byte(data))
			if err != nil {
				log.Printf("Client write error: %v", err)
				return
			}
			c.Writer.Flush()
		}
	}
}