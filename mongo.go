package webgo_modules

import (
	"errors"
	"sync"
	"time"

	"github.com/IntelliQru/logger"
	"gopkg.in/mgo.v2"
)

var mongo *MongoCluster

type (
	MongoConnection struct {
		Id       string
		Host     string
		Name     string
		Login    string
		Password string
		Session  *mgo.Session

		mode   mgo.Mode
		once   sync.Once
		locker sync.RWMutex
	}

	MongoCluster struct {
		connections map[string]MongoConnection
		poolLimit   int
		logger      *logger.Logger
	}
)

func (m *MongoConnection) SetMode(mode mgo.Mode) {
	m.internalInit()

	m.locker.Lock()
	m.mode = mode
	m.locker.Unlock()
}

func (m *MongoConnection) Mode() (mode mgo.Mode) {
	m.internalInit()

	m.locker.RLock()
	mode = m.mode
	m.locker.RUnlock()

	return mode
}

func (m *MongoConnection) internalInit() {
	m.once.Do(func() {
		m.locker.Lock()
		m.mode = mgo.Monotonic
		m.locker.Unlock()
	})
}

func NewMongoCluster(log *logger.Logger) *MongoCluster {
	if mongo == nil {
		mongo = &MongoCluster{
			logger:      log,
			connections: make(map[string]MongoConnection),
			poolLimit:   4096,
		}
	}

	return mongo
}

func GetMongoCluster() *MongoCluster {
	return mongo
}

func (m *MongoCluster) NewConnection(conn MongoConnection) (sess *mgo.Session, err error) {

	if conn.Host == "" {
		m.logger.Fatal("Unknow host")
	}

	sess, err = mgo.Dial(conn.Host)

	if err != nil {
		m.logger.Error("Error connecting mongodb on server "+conn.Host, err)
		return
	}

	err = sess.DB(conn.Name).Login(conn.Login, conn.Password)
	if err != nil {
		m.logger.Error(err)
		return
	}

	//sess.SetCursorTimeout(time.Duration(conn.CursorTimeout) * time.Millisecond)
	sess.SetSyncTimeout(time.Second)
	sess.SetPoolLimit(m.poolLimit)
	sess.SetMode(conn.Mode(), true)

	conn.Session = sess

	m.connections[conn.Id] = conn

	return
}

func (m *MongoCluster) GetConnection(id string) (sess *mgo.Session, err error) {

	conn, ok := m.connections[id]
	if !ok {
		err = errors.New("Connection ID not found")
		return
	}

	sess = conn.Session
	conn.locker.Lock()
	if sess.Ping() != nil {
		sess.Refresh()
	}
	conn.locker.Unlock()

	return
}
