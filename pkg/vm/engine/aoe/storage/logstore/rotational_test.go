package logstore

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRotation(t *testing.T) {
	rot, err := NewRotational(
		"/tmp/testrotation",
		"store",
		".rot",
		10)
	assert.Nil(t, err)

	var bs bytes.Buffer
	bs.WriteString("Hello ")
	bs.WriteString("World")
	_, err = rot.Write(bs.Bytes())
	assert.NotNil(t, err)

	bs.Reset()
	bs.WriteString("Hi")
	bs.WriteString("World")
	_, err = rot.Write(bs.Bytes())
	assert.Nil(t, err)
	bs.Reset()
	bs.WriteString("Hello")
	bs.WriteString("World")
	_, err = rot.Write(bs.Bytes())
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), rot.currVersion)
}
