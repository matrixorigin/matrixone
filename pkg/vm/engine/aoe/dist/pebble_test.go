package dist

import (
	"fmt"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestPebbleStorage(t *testing.T) {
	a := "hello world"
	p := "hello"
	println(strings.HasPrefix(a, p))

	s, err := pebble.NewStorage("./pebble-test/data")
	defer s.Close()
	assert.NoError(t, err)

	t.Run("prefix", func(t *testing.T) {
		prefix := "/m/db"
		for i := 1; i <= 3; i++ {
			key := []byte(fmt.Sprintf("%v/%v/%d", prefix, "defaultdb", i))
			err := s.Set(key, []byte{byte(0)})
			assert.NoError(t, err)
		}
		err := s.PrefixScan([]byte(fmt.Sprintf("%v/%v", prefix, "defaultdb")),
			func(key, value []byte) (bool, error) {
				println(string(key), value[0])
				return true, nil
			}, false)
		assert.NoError(t, err)
	})

	t.Run("scan", func(t *testing.T) {
		prefix := "/m/db"
		for i := 1; i <= 3; i++ {
			key := []byte(fmt.Sprintf("%v/%v/%d", prefix, "defaultdb", i))
			err := s.Set(key, []byte{byte(1)})
			assert.NoError(t, err)
		}
		err := s.Scan([]byte(fmt.Sprintf("%v/%v/%d", prefix, "defaultdb", 1)),
			[]byte(fmt.Sprintf("%v/%v/%d", prefix, "defaultdb", 4)),
			func(key, value []byte) (bool, error) {
				println(string(key), value[0])
				return true, nil
			},
			false)
		assert.NoError(t, err)
	})

	t.Run("get set", func(t *testing.T) {
		pairs := [][]byte{
			[]byte("k1"),
			[]byte("v1"),
			[]byte("k2"),
			[]byte("v2"),
		}
		err := s.BatchSet(pairs...)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, s.BatchDelete(pairs...))
			value, err := s.Get([]byte("k1"))
			assert.NoError(t, err)
			assert.Nil(t, value)
		}()
		value, err := s.Get([]byte("k1"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("v1"), value)
	})
}
