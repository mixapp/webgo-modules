package webgo_modules

import (
	"encoding/json"
	"github.com/IntelliQru/logger"
	"github.com/streadway/amqp"
	"net"
	"time"
)

var log *logger.Logger

var rabbitCluster *RabbitCluster

type (
	RabbitConnection struct {
		id        string
		conn      *amqp.Connection
		ch        *amqp.Channel
		ampq      string
		queues    map[string]*Queue
		done      chan bool
		exchanges []*Exchange
		Qos       int
	}

	Exchange struct {
		Name       string
		Type       string
		Durable    bool
		Autodelete bool
	}

	Queue struct {
		Name       string
		Key        string
		Exchange   string
		Durable    bool
		Autodelete bool
		Handler    func(dl *amqp.Delivery)
		/*		queueMQ    *amqp.Queue*/
		done   chan bool
		error  chan error
		inChan <-chan amqp.Delivery
	}

	RabbitCluster struct {
		connections map[string]*RabbitConnection
	}
)

func NewRabbitCluster(logger *logger.Logger) *RabbitCluster {
	log = logger

	if rabbitCluster == nil {
		rabbitCluster = &RabbitCluster{
			connections: make(map[string]*RabbitConnection),
		}
	}

	return rabbitCluster
}

func GetRabbitCluster() *RabbitCluster {
	return rabbitCluster
}

func NewExchange() *Exchange {
	e := Exchange{}
	return &e
}

func (r *RabbitCluster) NewConnection(id string, amqp string) *RabbitConnection {
	rConn := RabbitConnection{
		id:   id,
		ampq: amqp,
		Qos:  1,
	}

	r.connections[id] = &rConn
	return &rConn
}

func (r *RabbitCluster) GetConnection(clusterId string) *RabbitConnection {
	return r.connections[clusterId]
}

func (r *RabbitConnection) AddExchange(exchange *Exchange) {
	log.Debug("Add Exchange", exchange.Name)
	r.exchanges = append(r.exchanges, exchange)
}

func (r *RabbitConnection) ServeMQ() {
	log.Debug("Connect to RabbitMQ, ID:", r.id)
	r.done = make(chan bool)
	//	r.error = make(chan error)

	for {
		err := r.connect()
		if err != nil {
			if r.conn != nil {
				r.conn.Close()
			}
			if r.ch != nil {
				r.ch.Close()
			}

			time.Sleep(time.Second * 5)
		}
		log.Debug("Reconnect to RabbitMQ, ID:", r.id)
	}
}

func (r *RabbitConnection) AddQueue(q *Queue) {
	if r.queues == nil {
		r.queues = make(map[string]*Queue)
	}

	r.queues[q.Name] = q
}

func (r *RabbitConnection) connect() (err error) {
	r.conn, err = amqp.DialConfig(r.ampq, amqp.Config{
		Heartbeat: 2 * time.Second,
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, 2*time.Second)
		},
	})

	//На всякий случай, если кто-то решил не через метод создать
	if r.Qos == 0 {
		r.Qos = 1
	}

	//r.conn, err = amqp.Dial(r.ampq)
	if err != nil {
		log.Error(err)
		return err
	}

	r.ch, err = r.conn.Channel()
	if err != nil {
		log.Error(err)
		return err
	}

	if err = r.ch.Confirm(false); err != nil {
		log.Error(err)
		return err
	}

	//r.ackChn, r.nackChn = r.ch.NotifyConfirm(make(chan uint64, 1), make(chan uint64, 1))


	err = r.ch.Qos(r.Qos, 0, true)
	if err != nil {
		log.Error(err)
		return err
	}

	// Объявляем обменники
	for _, exchange := range r.exchanges {
		if err := r.ch.ExchangeDeclare(exchange.Name, exchange.Type, exchange.Durable, exchange.Autodelete, false, false, nil); err != nil {
			log.Error(err)
			return err
		}
	}

	for key := range r.queues {

		_, err := r.ch.QueueDeclare(r.queues[key].Name, r.queues[key].Durable, r.queues[key].Autodelete, false, false, nil)
		if err != nil {
			log.Error(err)
			return err
		}

		// Привязываем очередь к обменнику
		if len(r.queues[key].Exchange) != 0 {
			err = r.ch.QueueBind(r.queues[key].Name, r.queues[key].Key, r.queues[key].Exchange, false, nil)
			if err != nil {
				log.Error(err)
				return err
			}
		}

		r.queues[key].inChan, err = r.ch.Consume(r.queues[key].Name, "", false, false, false, false, nil)
		if err != nil {
			log.Error(err)
			return err
		}

		/*r.queues[key].queueMQ = &q*/
		r.queues[key].done = r.done
		go r.queues[key].listenQueue()
	}

	<-r.done

	return
}

func (r *RabbitConnection) Publish(exchange, routeKey string, data interface{}) (err error) {

	body, err := json.Marshal(data)
	if err != nil {
		return
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         body,
	}

	tries := 0

	for tries < 10 {
		err = r.ch.Publish(exchange, routeKey, true, false, msg)
		if err != nil {
			log.Error("Error publish message, tries: ", tries)
			tries++
			r.done <- true
			continue
		}
		break
	}

	return
}

func (q *Queue) listenQueue() {
	log.Debug("Listen queue")
	defer func() {
		q.done <- true
	}()
	for dl := range q.inChan {
		go q.Handler(&dl)
	}

	return
}
