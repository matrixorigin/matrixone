package utils

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func WithUplimitSensorOption[T types.OrderedT](upLimit T) SensorOption[T] {
	return func(s *Sensor[T]) {
		s.upLimit = upLimit
		s.hasUpLimit = true
	}
}

func WithDownlimitSensorOption[T types.OrderedT](downLimit T) SensorOption[T] {
	return func(s *Sensor[T]) {
		s.downLimit = downLimit
		s.hasDownLimit = true
	}
}

func NewSensor[T types.OrderedT](opts ...SensorOption[T]) *Sensor[T] {
	s := &Sensor[T]{}
	for _, opt := range opts {
		opt(s)
	}
	if s.hasUpLimit && s.hasDownLimit {
		if s.upLimit < s.downLimit {
			panic(fmt.Sprintf("upLimit %v < downLimit %v", s.upLimit, s.downLimit))
		}
	}
	return s
}

func (s *Sensor[T]) Add(v T) T {
	ptr := s.current.Load()
	var newValue T
	if ptr != nil {
		newValue = v + *ptr
	} else {
		newValue = v
	}
	if !s.current.CompareAndSwap(ptr, &newValue) {
		for {
			ptr = s.current.Load()
			if ptr != nil {
				newValue = v + *ptr
			} else {
				newValue = v
			}
			if s.current.CompareAndSwap(ptr, &newValue) {
				break
			}
		}
	}

	return newValue
}

func (s *Sensor[T]) GetCurrentValue() T {
	ptr := s.current.Load()
	if ptr == nil {
		var v T
		return v
	}
	return *ptr
}

func (s *Sensor[T]) HasUpLimit() bool {
	return s.hasUpLimit
}

func (s *Sensor[T]) HasDownLimit() bool {
	return s.hasDownLimit
}

func (s *Sensor[T]) HasUpOverflow() bool {
	if !s.hasUpLimit {
		return false
	}
	return s.GetCurrentValue() > s.upLimit
}

func (s *Sensor[T]) HasDownOverflow() bool {
	if !s.hasDownLimit {
		return false
	}
	return s.GetCurrentValue() < s.downLimit
}

func (s *Sensor[T]) Reset() {
	s.current.Store(nil)
}
