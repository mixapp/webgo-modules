package webgo_modules

import (
	"testing"

	"github.com/stretchr/testify/assert"
	mgo "gopkg.in/mgo.v2"
)

func TestMongoConnectionMode(t *testing.T) {

	{
		m := new(MongoConnection)

		for _, mode := range []mgo.Mode{
			mgo.Strong,
			mgo.Monotonic,
			mgo.Eventual,
		} {

			m.SetMode(mode)
			assert.Equal(t, mode, m.Mode())
		}
	}

	{
		m := new(MongoConnection)
		assert.Equal(t, mgo.Monotonic, m.Mode())

		m.SetMode(mgo.Strong)
		assert.Equal(t, mgo.Strong, m.Mode())
	}
}
