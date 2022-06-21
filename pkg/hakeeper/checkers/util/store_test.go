package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClusterStores(t *testing.T) {
	clusterStores := NewClusterStores()
	clusterStores.RegisterWorking(NewStore("a", 0, 0))
	assert.Equal(t, &Store{ID: "a"}, clusterStores.WorkingStores()[0])

	clusterStores.RegisterExpired(NewStore("b", 0, 0))
	assert.Equal(t, &Store{ID: "b"}, clusterStores.ExpiredStores()[0])
}
