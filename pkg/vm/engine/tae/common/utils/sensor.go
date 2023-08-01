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
		sensors: make(map[string]Sensor),
	}
}

func RegisterSensor(sensor Sensor) {
	DefaultSensorRegistry.Lock()
	defer DefaultSensorRegistry.Unlock()
	if _, ok := DefaultSensorRegistry.sensors[sensor.Name()]; ok {
		panic(fmt.Sprintf("sensor %s already exists", sensor.Name()))
	}
	DefaultSensorRegistry.sensors[sensor.Name()] = sensor
}

func UnregisterSensor(sensor Sensor) {
	DefaultSensorRegistry.Lock()
	defer DefaultSensorRegistry.Unlock()
	if _, ok := DefaultSensorRegistry.sensors[sensor.Name()]; !ok {
		panic(fmt.Sprintf("sensor %s does not exist", sensor.Name()))
	}
	delete(DefaultSensorRegistry.sensors, sensor.Name())
}

func (r *SensorRegistry) GetSensor(name string) Sensor {
	r.RLock()
	defer r.RUnlock()
	return r.sensors[name]
}

func (r *SensorRegistry) ForEach(f func(sensor Sensor)) {
	r.RLock()
	defer r.RUnlock()
	for _, sensor := range r.sensors {
		f(sensor)
	}
}

func WithGetStateSensorOption[T types.OrderedT](fn func(T) SensorState) SensorOption[T] {
	return func(s *NumericSensor[T]) {
		s.getStateFn = fn
	}
}

func NewNumericSensor[T types.OrderedT](name string, opts ...SensorOption[T]) *NumericSensor[T] {
	s := &NumericSensor[T]{
		name: name,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *NumericSensor[T]) Add(v T) T {
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

func (s *NumericSensor[T]) GetCurrentValue() T {
	ptr := s.current.Load()
	if ptr == nil {
		var v T
		return v
	}
	return *ptr
}

func (s *NumericSensor[T]) IsRed() bool {
	return s.State() == SensorStateRed
}

func (s *NumericSensor[T]) State() SensorState {
	if s.getStateFn != nil {
		return s.getStateFn(s.GetCurrentValue())
	}
	return SensorStateGreen
}

func (s *NumericSensor[T]) Reset() {
	s.current.Store(nil)
}

func (s *NumericSensor[T]) Name() string {
	return s.name
}

func (s *NumericSensor[T]) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("NumericSensor[\"%s\"]", s.name))
	_, _ = w.WriteString(fmt.Sprintf(" Current:%v", s.GetCurrentValue()))
	_, _ = w.WriteString(fmt.Sprintf(" State:%s", s.State()))
	return w.String()
}

// SimpleSensor

func NewSimpleSensor(name string) *SimpleSensor {
	return &SimpleSensor{
		name: name,
	}
}

func (s *SimpleSensor) IsRed() bool {
	return s.State() == SensorStateRed
}

func (s *SimpleSensor) State() SensorState {
	state := s.state.Load()
	return SensorState(state)
}

func (s *SimpleSensor) SetState(state SensorState) {
	s.state.Store(int32(state))
}

func (s *SimpleSensor) Name() string {
	return s.name
}

func (s *SimpleSensor) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("SimpleSensor[\"%s\"]", s.name))
	_, _ = w.WriteString(fmt.Sprintf(" State:%s", s.State()))
	return w.String()
}
