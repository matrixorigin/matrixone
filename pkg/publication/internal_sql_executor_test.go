package publication

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
)

func TestDefChangedBackoff_NormalRetry(t *testing.T) {
	b := &defChangedBackoff{
		baseInterval: time.Second,
		getLastErr:   func() error { return nil },
		getAttempt:   func() int { return 1 },
	}
	assert.Equal(t, time.Second, b.Next(1))
}

func TestDefChangedBackoff_DefChangedFirstAttempt(t *testing.T) {
	b := &defChangedBackoff{
		baseInterval: time.Second,
		getLastErr:   func() error { return moerr.NewTxnNeedRetryWithDefChangedNoCtx() },
		getAttempt:   func() int { return 1 },
	}
	assert.Equal(t, 2*time.Second, b.Next(1))
}

func TestDefChangedBackoff_DefChangedLaterAttempt(t *testing.T) {
	b := &defChangedBackoff{
		baseInterval: time.Second,
		getLastErr:   func() error { return moerr.NewTxnNeedRetryWithDefChangedNoCtx() },
		getAttempt:   func() int { return 2 },
	}
	// attempt > 1, so regular interval
	assert.Equal(t, time.Second, b.Next(2))
}

func TestDefChangedBackoff_AttemptLessThanOne(t *testing.T) {
	b := &defChangedBackoff{
		baseInterval: time.Second,
		getLastErr:   func() error { return nil },
		getAttempt:   func() int { return 0 },
	}
	// attempt < 1 gets clamped to 1
	assert.Equal(t, time.Second, b.Next(0))
}
