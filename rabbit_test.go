package webgo_modules

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IntelliQru/logger"
	"github.com/streadway/amqp"
)

func TestSendGet(t *testing.T) {

	wg := sync.WaitGroup{}
	testValue := time.Now().Format("2006-01-02T15:04:05.999999-07:00")
	queueName := "TestSendGet"

	handler := func(dl *amqp.Delivery) {

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
		Handler:    handler,
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

func TestCrash(t *testing.T) {

	var (
		countMessaggesProcess int
		countMessaggesSend    int
	)

	const (
		countThreads  = 1000
		countMessages = 100
	)

	testValue := time.Now().Format("2006-01-02T15:04:05.999999-07:00")

	// HANDLERS

	pushHandler := func(dl *amqp.Delivery) {}
	readHandler := func(dl *amqp.Delivery) {
		defer func() {
			if errorText := recover(); errorText != nil {
				log.Error(fmt.Sprintln(errorText))
			}
		}()

		dl.Ack(true)
		countMessaggesProcess += 1

		var message interface{}
		err := json.Unmarshal(dl.Body, &message)
		if err != nil {
			t.Error(err.Error())
		}

		if testValue != message {
			t.Errorf("Wrong value '%v' != '%v'", testValue, message)
		}

		if countMessaggesProcess%(countThreads*countMessages) == 1 {
			panic("Run handler crush test")
		}
	}

	// QUEUES
	queues := make([]*Queue, 0)
	for i := 0; i < 2; i++ {
		n := fmt.Sprintf("queue_%d", i)
		q := Queue{Name: n, Key: n + "_key", Exchange: "", Durable: true, Autodelete: false, Handler: readHandler}
		queues = append(queues, &q)
	}

	// CONNECTION

	conn := getConnection()

	defer func() {
		for _, q := range conn.queues {
			conn.ch.QueueDelete(q.Name, false, false, false)
		}
		conn.Close()
	}()

	// FUNCTION FOR CRUSH

	crash := func(chStop <-chan bool) {
		for {
			select {
			case <-chStop:
				return
			default:
				if countMessaggesProcess%countThreads == 1 || countMessaggesSend == 1 {
					time.Sleep(time.Second)
					if conn.getState() != CONNECTION_WORK {
						continue
					} else {
						time.Sleep(time.Second * 10)
					}

					conn.setState(CONNECTION_CRUSH) // crash test
				}
			}
		}
	}

	sendMessageToRabbit := func(wg *sync.WaitGroup, rabbitConn *RabbitConnection, queueName string, countMessages int, val interface{}) {
		wg.Add(1)
		defer wg.Done()

		for i := 0; i < countMessages; i++ {
			if err := rabbitConn.Publish("", queueName, val); err != nil {
				t.Error(err)
				return
			}
		}
	}

	// TEST

	chStop := make(chan bool)

	for testIndex := 0; testIndex < 3; testIndex++ {
		countMessaggesProcess = 0

		fmt.Println("=====================")
		fmt.Println("Write to RabbitMQ queues")
		fmt.Println("=====================")

		setRabbitQueues(conn, queues, pushHandler)
		go crash(chStop)

		wg := new(sync.WaitGroup)

		for thread := 0; thread < countThreads; thread++ {
			for name, _ := range conn.queues {
				go sendMessageToRabbit(wg, conn, name, countMessages, testValue)
			}
		}

		wg.Wait()

		chStop <- true
		countMessaggesSend = 0

		for name, _ := range conn.queues {
			if info, err := conn.ch.QueueInspect(name); err == nil {
				countMessaggesSend += info.Messages
			} else {
				t.Error(err)
				return
			}
		}

		fmt.Println("=====================")
		fmt.Println("Read queues, messages in the RabbitMQ storage:", countMessaggesSend)
		fmt.Println("=====================")

		setRabbitQueues(conn, queues, readHandler)

		go crash(chStop)

		for {
			time.Sleep(time.Second)

			var countEnd int

			for name, _ := range conn.queues {
				info, err := conn.ch.QueueInspect(name)
				if err == nil && info.Messages == 0 {
					countEnd += 1
				} else {
					fmt.Printf("Waiting empty queues. Queue state: %+v; error: %v\n", info, err)
				}
			}

			if countEnd == len(conn.queues) {
				chStop <- true
				break
			}
		}

		result := fmt.Sprintf("RabbitMQ messages: process=%d; in the storage=%d; diff=%d", countMessaggesProcess, countMessaggesSend, countMessaggesSend-countMessaggesProcess)

		fmt.Println("\n\ntest:", testIndex, result, "\n\n")

		// If the read messages more than sent, that means the channel was shut down before in the message has been set attribute "Read"
		if countMessaggesProcess < countMessaggesSend {
			t.Error(result)
			return
		}
	}
}

func setRabbitQueues(conn *RabbitConnection, queues []*Queue, hdl func(*amqp.Delivery)) {

	conn.setState(CONNECTION_STOP)
	for {
		time.Sleep(time.Second)
		if conn.getState() == CONNECTION_STOP || conn.getState() == CONNECTION_NOT_WORK {
			break
		}
	}

	conn.queues = nil

	for _, q := range queues {
		tmp := *q
		tmp.Handler = hdl
		conn.AddQueue(&tmp)
	}

	go conn.ServeMQ()

	for {
		time.Sleep(time.Second)
		if conn.getState() == CONNECTION_WORK {
			break
		}
	}
}

func getConnection() *RabbitConnection {

	cp := logger.ConsoleProvider{}

	log := logger.NewLogger()
	log.SetLevel(2) // 2 -DEBUG
	log.RegisterProvider(cp)

	log.AddLogProvider(cp.GetID())
	log.AddErrorProvider(cp.GetID())
	log.AddFatalProvider(cp.GetID())
	log.AddDebugProvider(cp.GetID())

	rabbitMQ := NewRabbitCluster(log)
	url := "amqp://guest:guest@127.0.0.1:5672/"
	conn := rabbitMQ.NewConnection("test_connection", url)

	return conn
}
