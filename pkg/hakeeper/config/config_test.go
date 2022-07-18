package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeoutConfig(t *testing.T) {
	c := DefaultTimeoutConfig()
	assert.Equal(t, DefaultTickPerSecond, c.TickPerSecond)
	assert.Equal(t, DefaultLogStoreTimeout, c.LogStoreTimeout)
	assert.Equal(t, DefaultDnStoreTimeout, c.DnStoreTimeout)

	nc := DefaultTimeoutConfig().SetLogStoreTimeout(time.Minute)
	assert.Equal(t, time.Minute, nc.LogStoreTimeout)

	nc = nc.SetTickPerSecond(100)
	assert.Equal(t, 100, nc.TickPerSecond)

	nc = nc.SetDnStoreTimeout(100 * time.Second)
	assert.Equal(t, 100*time.Second, nc.DnStoreTimeout)
}
