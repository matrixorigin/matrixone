package utils

import (
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type SensorState int8

func (s SensorState) String() string {
	switch s {
	case SensorStateGreen:
		return "Green"
	case SensorStateYellow:
		return "Yellow"
	case SensorStateRed:
		return "Red"
	default:
		return "UnknownState"
	}
}

const (
	SensorStateGreen  SensorState = iota
	SensorStateYellow             = 1
	SensorStateRed                = 2
)

var DefaultSensorRegistry = NewSensorRegistry()

type SensorRegistry struct {
	sync.RWMutex
	sensors map[string]Sensor
}

type Sensor interface {
	Name() string
	State() SensorState
	IsRed() bool
	String() string
}

type SensorOption[T types.OrderedT] func(*NumericSensor[T])

type NumericSensor[T types.OrderedT] struct {
	getStateFn func(T) SensorState
	current    atomic.Pointer[T]
	name       string
}
