package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

const (
	topic          = "my-kafka-topic"
	broker1Address = "localhost:9093"
)

func producer(kafkaWriter *kafka.Writer, ctx context.Context) {
	var body string
	for {
		fmt.Scanf("%v", &body)
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("address-%s", broker1Address)),
			Value: []byte(body),
		}
		err := kafkaWriter.WriteMessages(ctx, msg)
		if err != nil {
			log.Fatalln(err)
		}
	}

}

func getKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func consume(ctx context.Context, group string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address},
		Topic:   topic,
		GroupID: group,
	})

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		fmt.Printf("%v received: %v", group, string(msg.Value))
		fmt.Println()
	}
}
func main() {
	ctx := context.Background()
	go producer(getKafkaWriter(broker1Address, topic), ctx)
	go consume(ctx, "grp1")
	go consume(ctx, "grp2")
	go consume(ctx, "grp3")
	time.Sleep(time.Minute * 2)
}
