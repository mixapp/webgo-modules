package webgo_modules

import (
	"errors"
	"github.com/IntelliQru/logger"
	"gopkg.in/mgo.v2"
	"sync"
	"time"
)

var mongo *MongoCluster

type (
	MongoConnection struct {
		Id            string
		Host          string
		Name          string
		Login         string
		Password      string
		Session       *mgo.Session
		CursorTimeout int
		locker        sync.Mutex
	}

	MongoCluster struct {
		connections map[string]MongoConnection
		poolLimit   int
		logger      *logger.Logger
	}
)

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

	sess.SetCursorTimeout(time.Duration(conn.CursorTimeout) * time.Millisecond)
	sess.SetSyncTimeout(time.Second)
	sess.SetPoolLimit(m.poolLimit)
	sess.SetMode(mgo.Monotonic, true)

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
