package objectio

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewObjectWriter(t *testing.T) {
	writer, err := NewObjectWriter("tesfsdf")
	assert.NotNil(t, writer)
	assert.Nil(t, err)
}
