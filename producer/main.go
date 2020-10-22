package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	fmt.Println("Producer")
	producerLoop(newAsyncProducer())
}

func producerLoop(producer sarama.AsyncProducer) {
	reader := bufio.NewReader(os.Stdin)
	for {
		text := getMessage(reader)
		sendMessage(producer, text)
	}
}

func sendMessage(producer sarama.AsyncProducer, message string) {
	topic := os.Getenv("KAFKA_TOPIC")
	log.Printf("[producer] Topic{%s} Message: %s", topic, message)
	producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
}

func getMessage(reader *bufio.Reader) string {
	fmt.Print("-> ")
	message, err := reader.ReadString('\n')
	if err != nil {
		panic("cannot read the line!")
	}
	message = strings.Replace(message, "\n", "", -1)
	return message
}

func getDummyMessage() string {
	return "Dummy Message"
}

func newAsyncProducer() sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	producer, err := sarama.NewAsyncProducer(getBrokersSlice(), config)
	if err != nil {
		panic(err)
	}

	go func() {
		for err := range producer.Errors() {
			log.Fatalln("Failed to start sarama producer: ", err)
		}
	}()

	return producer
}

func getBrokersSlice() []string {
	brokers := os.Getenv("KAFKA_BROKERS")
	return strings.Split(brokers, ",")
}
