package webgo_modules

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/IntelliQru/logger"
	"github.com/streadway/amqp"
)

var (
	log           *logger.Logger
	rabbitCluster *RabbitCluster
)

type ConnectionState uint8

const (
	CONNECTION_NOT_WORK  ConnectionState = 0
	CONNECTION_WORK      ConnectionState = 1
	CONNECTION_STOP      ConnectionState = 2
	CONNECTION_RECONNECT ConnectionState = 3
	CONNECTION_CRUSH     ConnectionState = 4 // only tests
)

type RabbitCluster struct {
	connections map[string]*RabbitConnection
}

type RabbitConnection struct {
	io.Closer
	mutex sync.RWMutex
	state ConnectionState

	id        string
	conn      *amqp.Connection
	ch        *amqp.Channel
	ampq      string
	queues    map[string]*Queue
	exchanges []*Exchange
	Qos       int
}

type Exchange struct {
	Name       string
	Type       string
	Durable    bool
	Autodelete bool
}

type Queue struct {
	Name       string
	Key        string
	Exchange   string
	Durable    bool
	Autodelete bool
	Handler    func(dl *amqp.Delivery)
	inChan     <-chan amqp.Delivery
	mutex      sync.RWMutex
}

// QUEUE

func (q *Queue) listenQueue() {

	log.Debug("Listen queue:", q.Name)
	defer func() {
		q.setCachannel(nil)
		log.Debug("-Listen queue:", q.Name)
	}()

	for {
		if dl, open := <-q.inChan; !open {
			return
		} else {
			go q.Handler(&dl)
		}
	}
}

func (q Queue) active() bool {
	q.mutex.RLock()
	defer q.mutex.RUnlock()

	return q.inChan != nil
}

func (q *Queue) setCachannel(ch <-chan amqp.Delivery) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.inChan = ch
}

// EXCHANGE

func NewExchange() *Exchange {
	return new(Exchange)
}

// CLUSTER

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

func (r *RabbitCluster) NewConnection(id string, amqp string) *RabbitConnection {

	conn := &RabbitConnection{
		id:   id,
		ampq: amqp,
		Qos:  1, // https://godoc.org/github.com/streadway/amqp#Channel.Qos
	}

	if r.connections == nil {
		r.connections = make(map[string]*RabbitConnection)
	}

	r.connections[id] = conn
	return conn
}

func (r *RabbitCluster) GetConnection(clusterId string) *RabbitConnection {

	if r.connections == nil {
		return nil
	} else if val, ok := r.connections[clusterId]; !ok {
		return nil
	} else {
		return val
	}
}

// CONNECTION

func (r *RabbitConnection) Close() error {
	r.setState(CONNECTION_STOP)
	return nil
}

func (r *RabbitConnection) AddExchange(exchange *Exchange) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	log.Debug("Add exchange:", exchange.Name)
	r.exchanges = append(r.exchanges, exchange)

	r.state = CONNECTION_RECONNECT
}

func (r *RabbitConnection) AddQueue(q *Queue) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	log.Debug("Add queue:", q.Name)

	if r.queues == nil {
		r.queues = make(map[string]*Queue)
	}

	r.queues[q.Name] = q
	r.state = CONNECTION_RECONNECT
}

func (r *RabbitConnection) Publish(exchange, routeKey string, data interface{}) (err error) {

	defer func() {
		if errorText := recover(); errorText != nil {
			// if connection is closed or now reconnecting
			err = errors.New("Failed send push to RabbitMQ: " + fmt.Sprintln(errorText))
			log.Error(err)
		}
	}()

	var body []byte

	if val, e := json.Marshal(data); e != nil {
		return errors.New("Failed convert source data to json:" + e.Error())
	} else {
		body = val
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         body,
	}

	for tries := 0; tries < 10; tries++ {
		if err = r.ch.Publish(exchange, routeKey, true, false, msg); err != nil {
			log.Error("Failed publish message to RabbitMQ, tries: ", tries)

			switch r.getState() {
			case CONNECTION_WORK, CONNECTION_NOT_WORK:
				time.Sleep(time.Second)
				r.setState(CONNECTION_RECONNECT)
			}

			time.Sleep(time.Second) // waiting reconnect
		} else {
			break // all right
		}
	}

	return
}
func (r *RabbitConnection) ServeMQ() {

	for {
		log.Debug("Connect to RabbitMQ, ID:", r.id)

		r.mutex.Lock()
		state := r.state
		r.state = CONNECTION_NOT_WORK
		r.mutex.Unlock()

		if state == CONNECTION_STOP {
			break
		}

		if err := r.connect(); err != nil {
			time.Sleep(time.Second / 2)
		}

		if state != CONNECTION_STOP {
			log.Debug("Reconnect to RabbitMQ, ID:", r.id)
		}
	}
}

func (r *RabbitConnection) connect() (err error) {

	defer func() {
		if errorText := recover(); errorText != nil {
			err = errors.New(fmt.Sprintln(errorText))
			log.Error(err)
		}

		if r.ch != nil {
			r.ch.Close()
		}

		if r.conn != nil {
			r.conn.Close()
		}

		log.Debug("RabbitMQ connection close")
	}()

	connCfg := amqp.Config{
		Heartbeat: 2 * time.Second,
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, 2*time.Second)
		},
	}

	if r.conn, err = amqp.DialConfig(r.ampq, connCfg); err != nil {
		log.Error(err)
		return err
	} else if r.ch, err = r.conn.Channel(); err != nil {
		log.Error(err)
		return err
	} else if err = r.ch.Confirm(false); err != nil {
		log.Error(err)
		return err
	} else if err = r.ch.Qos(r.Qos, 0, true); err != nil {
		log.Error(err)
		return err
	}

	// Declaring exchangers
	for _, exchange := range r.exchanges {
		if err = r.ch.ExchangeDeclare(
			exchange.Name,
			exchange.Type,
			exchange.Durable,
			exchange.Autodelete,
			false, // internal
			false, // nowait
			nil,   // args
		); err != nil {
			log.Error(err)
			return err
		}
	}

	for _, queue := range r.queues {

		if _, err = r.ch.QueueDeclare(
			queue.Name,
			queue.Durable, // duration (note: not durable)
			queue.Autodelete,
			false, // exclusive
			false, // nowait
			nil,   // args
		); err != nil {
			log.Error(err)
			return err
		}

		if len(queue.Exchange) != 0 {
			// Linking queries and exchangers
			if err = r.ch.QueueBind(queue.Name, queue.Key, queue.Exchange, false, nil); err != nil {
				log.Error(err)
				return err
			}
		}

		if ch, err := r.ch.Consume(queue.Name, "", false, false, false, false, nil); err != nil {
			log.Error(err)
			return err
		} else {
			queue.setCachannel(ch)
		}

		if info, err := r.ch.QueueInspect(queue.Name); err == nil {
			log.Log("Messages in RabbitMQ queue:", queue.Name, "=", info.Messages)
		} else {
			log.Error(err)
			return err
		}

		go queue.listenQueue()
	}

	r.setState(CONNECTION_WORK)

	for {
		time.Sleep(time.Second / 10)

		if state := r.getState(); state == CONNECTION_STOP || state == CONNECTION_RECONNECT {
			return
		} else if state == CONNECTION_CRUSH {
			// Only for tests
			panic("Run connection crush test")
		}

		for _, queue := range r.queues {
			if !queue.active() {
				r.setState(CONNECTION_RECONNECT)
				return
			}
		}
	}

	return
}

func (r *RabbitConnection) setState(state ConnectionState) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.state = state
}

func (r RabbitConnection) getState() ConnectionState {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return r.state
}
