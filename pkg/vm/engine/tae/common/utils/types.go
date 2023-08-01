package utils

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type SensorOption[T types.OrderedT] func(*Sensor[T])

type Sensor[T types.OrderedT] struct {
	upLimit      T
	downLimit    T
	current      atomic.Pointer[T]
	hasUpLimit   bool
	hasDownLimit bool
}
