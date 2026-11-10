// Copyright 2021 - 2026 Matrix Origin
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

package function

import (
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func formatBytes(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStr[float64](parameters, result, proc, length, formatBytesValue, selectList)
}

func formatPicoTime(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStr[float64](parameters, result, proc, length, formatPicoTimeValue, selectList)
}

func formatBytesValue(value float64) string {
	magnitude := math.Abs(value)
	switch {
	case magnitude >= 1<<60:
		return formatScaledUnit(value/(1<<60), "EiB")
	case magnitude >= 1<<50:
		return formatScaledUnit(value/(1<<50), "PiB")
	case magnitude >= 1<<40:
		return formatScaledUnit(value/(1<<40), "TiB")
	case magnitude >= 1<<30:
		return formatScaledUnit(value/(1<<30), "GiB")
	case magnitude >= 1<<20:
		return formatScaledUnit(value/(1<<20), "MiB")
	case magnitude >= 1<<10:
		return formatScaledUnit(value/(1<<10), "KiB")
	case math.IsNaN(value):
		// MO's floating extensions have no MySQL finite-value counterpart.
		return fmt.Sprintf("%4g bytes", value)
	default:
		// The base-unit branch is bounded; MySQL truncates rather than rounds.
		return fmt.Sprintf("%4d bytes", int(value))
	}
}

func formatPicoTimeValue(value float64) string {
	magnitude := math.Abs(value)
	switch {
	case magnitude >= 86400e12:
		return formatScaledUnit(value/86400e12, "d")
	case magnitude >= 3600e12:
		return formatScaledUnit(value/3600e12, "h")
	case magnitude >= 60e12:
		return formatScaledUnit(value/60e12, "min")
	case magnitude >= 1e12:
		return formatScaledUnit(value/1e12, "s")
	case magnitude >= 1e9:
		return formatScaledUnit(value/1e9, "ms")
	case magnitude >= 1e6:
		return formatScaledUnit(value/1e6, "us")
	case magnitude >= 1e3:
		return formatScaledUnit(value/1e3, "ns")
	case math.IsNaN(value):
		return fmt.Sprintf("%4g ps", value)
	default:
		return fmt.Sprintf("%3d ps", int(value))
	}
}

func formatScaledUnit(value float64, unit string) string {
	// Choose notation before rounding, just as units are chosen before rounding.
	// Scientific notation also bounds output for the largest finite DOUBLE.
	if math.Abs(value) >= 100000 {
		return fmt.Sprintf("%4.2e %s", value, unit)
	}
	return fmt.Sprintf("%4.2f %s", value, unit)
}
