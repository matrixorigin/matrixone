package common

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestList(t *testing.T) {
	n0 := NewSLLNode(nil)
	var mu sync.RWMutex
	n1 := NewSLLNode(&mu)
	n0.SetNextNode(n1)
	n1.SetNextNode(nil)
	assert.Equal(t, n1, n0.GetNextNode())
	n2 := NewSLLNode(nil)
	n2.SetNextNode(nil)
	n0.SetNextNode(n2)
	assert.Equal(t, n2, n0.GetNextNode())
	n0.ReleaseNextNode()
	assert.Equal(t, nil, n0.GetNextNode())
}
