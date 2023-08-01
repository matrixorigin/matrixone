package utils

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var sequence atomic.Uint64

func MakeSensorName(
	module string, name string,
) string {
	return fmt.Sprintf("%s/%s:%d", module, name, sequence.Add(1))
}

func NewSensorRegistry() *SensorRegistry {
	return &SensorRegistry{
		sensors: make(map[string]*Sensor[float64]),
	}
}

func RegisterSensor(sensor *Sensor[float64]) {
	DefaultSensorRegistry.Lock()
	defer DefaultSensorRegistry.Unlock()
	if _, ok := DefaultSensorRegistry.sensors[sensor.name]; ok {
		panic(fmt.Sprintf("sensor %s already exists", sensor.name))
	}
	DefaultSensorRegistry.sensors[sensor.name] = sensor
}

func UnregisterSensor(sensor *Sensor[float64]) {
	DefaultSensorRegistry.Lock()
	defer DefaultSensorRegistry.Unlock()
	if _, ok := DefaultSensorRegistry.sensors[sensor.name]; !ok {
		panic(fmt.Sprintf("sensor %s does not exist", sensor.name))
	}
	delete(DefaultSensorRegistry.sensors, sensor.name)
}

func (r *SensorRegistry) GetSensor(name string) *Sensor[float64] {
	r.RLock()
	defer r.RUnlock()
	return r.sensors[name]
}

func (r *SensorRegistry) ForEach(f func(sensor *Sensor[float64])) {
	r.RLock()
	defer r.RUnlock()
	for _, sensor := range r.sensors {
		f(sensor)
	}
}

func WithGetStateSensorOption[T types.OrderedT](fn func(T) SensorState) SensorOption[T] {
	return func(s *Sensor[T]) {
		s.getStateFn = fn
	}
}

func NewSensor[T types.OrderedT](name string, opts ...SensorOption[T]) *Sensor[T] {
	s := &Sensor[T]{
		name: name,
	}
	for _, opt := range opts {
		opt(s)
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

func (s *Sensor[T]) IsRed() bool {
	return s.State() == SensorStateRed
}

func (s *Sensor[T]) State() SensorState {
	if s.getStateFn != nil {
		return s.getStateFn(s.GetCurrentValue())
	}
	return SensorStateGreen
}

func (s *Sensor[T]) Reset() {
	s.current.Store(nil)
}

func (s *Sensor[T]) Name() string {
	return s.name
}

func (s *Sensor[T]) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("Sensor[\"%s\"]", s.name))
	_, _ = w.WriteString(fmt.Sprintf(" Current:%v", s.GetCurrentValue()))
	_, _ = w.WriteString(fmt.Sprintf(" State:%s", s.State()))
	return w.String()
}
