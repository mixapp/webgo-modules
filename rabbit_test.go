package webgo_modules

import (
	"github.com/IntelliQru/logger"
	"github.com/streadway/amqp"

	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestSendGet(t *testing.T) {

	wg := sync.WaitGroup{}
	testValue := time.Now().Format("2006-01-02T15:04:05.999999-07:00")
	queueName := "TestSendGet"

	listen := func(dl *amqp.Delivery) {

		defer wg.Done()
		dl.Ack(true)

		var message interface{}
		err := json.Unmarshal(dl.Body, &message)
		if err != nil {
			t.Error(err.Error())
		}

		if testValue != message {
			t.Errorf("Wrong value '%v' != '%v'", testValue, message)
		}
	}

	queue := &Queue{
		Name:       queueName,
		Key:        "Key",
		Exchange:   "",
		Durable:    true,
		Autodelete: false,
		Handler:    listen,
	}

	conn := getConnection()
	conn.AddQueue(queue)
	go conn.ServeMQ()

	time.Sleep(time.Second) // Waiting initialize

	wg.Add(1)

	if err := conn.Publish("", queueName, testValue); err != nil {
		t.Error(err.Error())
	}

	wg.Wait()
}

func TestMultyThread(t *testing.T) {

	wg := sync.WaitGroup{}
	testValue := time.Now().Format("2006-01-02T15:04:05.999999-07:00")
	queueName := "TestMultyThread"

	listen := func(dl *amqp.Delivery) {
		defer wg.Done()
		dl.Ack(true)

		var message interface{}
		err := json.Unmarshal(dl.Body, &message)
		if err != nil {
			t.Error(err.Error())
		}

		if testValue != message {
			t.Errorf("Wrong value '%v' != '%v'", testValue, message)
		}
	}

	queue := &Queue{
		Name:       queueName,
		Key:        "Key",
		Exchange:   "",
		Durable:    true,
		Autodelete: false,
		Handler:    listen,
	}

	conn := getConnection()
	conn.AddQueue(queue)
	go conn.ServeMQ()

	time.Sleep(time.Second) // Waiting initialize

	for i := 0; i < 10000; i++ {
		wg.Add(1)

		go func() {
			if err := conn.Publish("", queueName, testValue); err != nil {
				t.Error(err.Error())
			}
		}()
	}

	wg.Wait()
}

func getConnection() *RabbitConnection {

	cp := logger.ConsoleProvider{}

	log := logger.NewLogger()
	log.RegisterProvider(cp)

	log.AddLogProvider(cp.GetID())
	log.AddErrorProvider(cp.GetID())
	log.AddFatalProvider(cp.GetID())
	log.AddDebugProvider(cp.GetID())

	rabbitMQ := NewRabbitCluster(log)
	url := "amqp://guest:guest@127.0.0.1:5672/"
	conn := rabbitMQ.NewConnection("connectionId", url)

	return conn
}
