package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

var kafkaWriter *kafka.Writer
var kafkaReader *kafka.Reader
var db *sql.DB

type KafkaMessage struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Offset    int64  `json:"offset"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

type EventPayload struct {
	User      string    `json:"username"`
	Event     string    `json:"event"`
	TimeStamp time.Time `json:"ts"`
}

func main() {
	//Set up Kafka writer
	kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{},
	})

	//Setting up Kafka reader
	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "test-topic",
		GroupID:   "go-api-consumer",
		Partition: 0,
		MinBytes:  10e3,
		MaxBytes:  10e6,
	})

	defer kafkaWriter.Close()

	var err error
	db, err = sql.Open("postgres", "postgres://postgres:pass@localhost:5433/postgres?sslmode=disable")
	if err != nil {
		log.Fatal("DB connection error:", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal("DB ping failed:", err)
	}
	log.Println("Connected to DB")

	//Set up GIN server
	router := gin.Default()

	//Route to handle produce Kafka message
	router.POST("/produce", func(c *gin.Context) {
		var payload EventPayload
		if err := c.BindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
			return
		}

		//Convert struct to JSON string to send as message
		msgBytes, err := json.Marshal(payload)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encode JSON"})
			return
		}

		writer := &kafka.Writer{
			Addr:     kafka.TCP("localhost:9092"),
			Topic:    "test-topic",
			Balancer: &kafka.LeastBytes{},
		}

		err = writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(payload.User),
			Value: msgBytes,
		},
		)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to send message"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "message sent"})
	})
	router.GET("/consume", consumeHandler)
	router.GET("/stream", streamHandler)

	router.LoadHTMLFiles("web/dashboard.html")
	router.GET("/dashboard", func(c *gin.Context) {
		c.HTML(200, "dashboard.html", nil)
	})
	log.Println("🚀 Listening at http://localhost:8080/dashboard")

	//Run API on port 8080
	log.Println("API running on localhost:8080")
	router.Run(":8080")
}

type MessageRequest struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

func produceHandler(c *gin.Context) {
	var req MessageRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON Request"})
		return
	}

	msg := kafka.Message{
		Key:   []byte(req.Key),
		Value: []byte(req.Value),
	}

	if err := kafkaWriter.WriteMessages(context.Background(), msg); err != nil {
		log.Printf("Error writing message to kafka: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Kafka write failed"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Message sent to Kafka", "key": req.Key})
}

func consumeHandler(c *gin.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"host.docker.internal:9092"},
		Topic:    "test-topic",
		GroupID:  "go-api-consumer-batch", //creating a batch consumer
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	defer reader.Close()

	var messages []KafkaMessage
	ctx := context.Background()

	//Consume upto 10 messages or stop after 3 seconds
	timeout := time.After(3 * time.Second)
	for i := 0; i < 10; i++ {
		select {
		case <-timeout:
			goto END
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Kafka read error: %v\n", err)
				goto END
			}

			messages = append(messages, KafkaMessage{
				Key:       string(msg.Key),
				Value:     string(msg.Value),
				Offset:    msg.Offset,
				Topic:     msg.Topic,
				Partition: msg.Partition,
			})
		}
	}

END:

	if len(messages) == 0 {
		c.JSON(http.StatusOK, gin.H{"message": "No messages available"})
		return
	}

	c.JSON(http.StatusOK, messages)
}

func streamHandler(c *gin.Context) {
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Flush()

	ctx := c.Request.Context()

	//Fetching past events from DB
	log.Println("Fetching past events from DB...")
	rows, err := db.Query("SELECT username, event, ts FROM kafka_events ORDER BY id DESC LIMIT 100")
	if err != nil {
		log.Println("DB query error:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {

		var username, event string
		var ts time.Time

		log.Printf("Sending past: user=%s event=%s time=%s\n", username, event, ts)

		if err := rows.Scan(&username, &event, &ts); err == nil {
			payload := EventPayload{
				User:      username,
				Event:     event,
				TimeStamp: time.Now(),
			}
			jsonData, _ := json.Marshal(payload)
			c.Writer.Write([]byte("data: " + string(jsonData) + "\n\n"))
			c.Writer.Flush()
		}
	}

	//Kafka Reader Setup
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "test-topic",
		GroupID:  "go-stream-consumer",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	//Now stream new messages from Kafka
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("Client disconnected")
				return
			default:
				msg, err := reader.ReadMessage(ctx)
				log.Printf("Received Kafka msg: %s\n", string(msg.Value))
				if err != nil {
					log.Printf("Kafka read error: %v\n", err)
					time.Sleep(1 * time.Second)
					continue
				}

				//Unmarshal kafka message into struct
				var payload EventPayload
				log.Printf("Parsed payload: %+v\n", payload)
				if err := json.Unmarshal(msg.Value, &payload); err != nil {
					log.Println("Invalid Kafka JSON:", err)
					continue
				}

				//Save to DB
				log.Println("Inserting to DB...")
				_, err = db.Exec(`INSERT INTO kafka_events(username, event, ts) VALUES ($1, $2, $3)`, payload.User, payload.Event, payload.TimeStamp)
				if err != nil {
					log.Println("DB insert error:", err)
					continue
				}

				//Send to frontend
				jsonData, _ := json.Marshal(payload)
				log.Println("Streaming to client...")
				c.Writer.Write([]byte("data: " + string(jsonData) + "\n\n"))
				c.Writer.Flush()
			}
		}
	}()

	//keeping connection open
	<-ctx.Done()
	log.Println("Closed SSE stream")

}
