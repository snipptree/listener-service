package main

import (
	"listener/event"
	"log"
	"math"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// try to connect to rabbitMq
	rabbitConn, err := connect()
	if err != nil {
		log.Printf("error connecting to rabbitmq: %v", err)
		os.Exit(1)
	}
	defer rabbitConn.Close()

	// start listening to messages
	log.Println("listening for and consuming RabbitMQ messages...")

	// create consumer
	consumer, err := event.NewConsumer(rabbitConn)
	if err != nil {
		panic(err)
	}

	// watch the queue and consume events
	err = consumer.Listen([]string{"log.INFO", "log.WARNING", "log.ERROR"})
	if err != nil {
		log.Println(err)
	}
}

func connect() (*amqp.Connection, error) {
	var counts int32
	var backOff = 1 * time.Second
	var connection *amqp.Connection

	// dont continue until our rabbit is ready
	for {
		c, err := amqp.Dial("amqp://guest:guest@rabbitmq")
		if err != nil {
			log.Println("rabbitmq not yet ready...")
			counts++
		} else {
			connection = c
			log.Println("Connected to Rabbit MQ!")
			break
		}

		if counts > 5 {
			log.Printf("error connecting to rabbitmq: %v", err)
		}

		backOff = time.Duration(math.Pow(float64(counts), 2)) * time.Second
		log.Println("backing off...")
		time.Sleep(backOff)
		continue
	}
	return connection, nil
}
