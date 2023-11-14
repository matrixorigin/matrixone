// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

type SimpleSensor struct {
	name  string
	state atomic.Int32
}
