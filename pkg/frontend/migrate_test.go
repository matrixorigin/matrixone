package frontend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMigrateController(t *testing.T) {
	mc := newMigrateController()
	assert.NotNil(t, mc)
}

func TestCloseOnly(t *testing.T) {
	mc := newMigrateController()
	assert.NotNil(t, mc)
	mc.waitAndClose()
}

func TestFirstCloseThenMigrate(t *testing.T) {
	mc := newMigrateController()
	assert.NotNil(t, mc)
	mc.waitAndClose()
	assert.Equal(t, mc.beginMigrate(), false)
}

func TestFirstMigrateThenClose(t *testing.T) {
	mc := newMigrateController()
	assert.NotNil(t, mc)
	assert.Equal(t, mc.beginMigrate(), true)
	mc.endMigrate()
	mc.waitAndClose()
}

func TestCloseWaitMigrate(t *testing.T) {
	mc := newMigrateController()
	assert.NotNil(t, mc)
	assert.Equal(t, mc.beginMigrate(), true)
	go func() {
		select {
		case <-time.After(time.Second):
			mc.endMigrate()
		}
	}()
	mc.waitAndClose()
}
