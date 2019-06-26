package main

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

type VcapServices struct {
	PRabbitMQ []PRabbitMQ `json:"p.rabbitmq"`
}

type PRabbitMQ struct {
	Credentials Credentials `json:"credentials"`
}

type Credentials struct {
	Protocols Protocols `json:"protocols"`
}

type Protocols struct {
	AMQP AMQP `json:"amqp"`
}

type AMQP struct {
	URI string `json:"uri"`
}

func getAMQPURI() string {
	var vcapServices VcapServices
	err := json.Unmarshal([]byte(os.Getenv("MY_VCAP_SERVICES")), &vcapServices)
	if err != nil {
		panic(err)
	}
	return vcapServices.PRabbitMQ[0].Credentials.Protocols.AMQP.URI
}

func consume(uri string) {
	conn, err := amqp.Dial(uri)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		panic(err)
	}
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		panic(err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func produce(uri string) {
	conn, err := amqp.Dial(uri)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		panic(err)
	}
	body := "Hello, RabbitMQ!"
	for {
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		if err != nil {
			panic(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func main() {
	uri := getAMQPURI()
	go produce(uri)
	consume(uri)
}
