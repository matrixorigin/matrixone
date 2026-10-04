// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"fmt"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
	PiB = 1024 * TiB
	EiB = 1024 * PiB

	THOUSAND    = 1000
	MILLION     = 1000 * THOUSAND
	BILLION     = 1000 * MILLION
	TRILLION    = 1000 * BILLION
	QUADRILLION = 1000 * TRILLION
)

func ConvertBytesToHumanReadable(bytes int64) string {
	num := float64(bytes)
	if bytes < KiB {
		return fmt.Sprintf("%d bytes", bytes)
	}
	if bytes < MiB {
		return fmt.Sprintf("%.2f KiB", num/KiB)
	}
	if bytes < GiB {
		return fmt.Sprintf("%.2f MiB", num/MiB)
	}
	if bytes < TiB {
		return fmt.Sprintf("%.2f GiB", num/GiB)
	}
	return fmt.Sprintf("%.2f TiB", num/TiB)
}

// ConvertUint64BytesToHumanReadable formats resource counters without narrowing
// them to int64. Explain output is presentation-only; persisted values remain
// raw bytes.
func ConvertUint64BytesToHumanReadable(bytes uint64) string {
	switch {
	case bytes < KiB:
		return fmt.Sprintf("%d bytes", bytes)
	case bytes < MiB:
		return fmt.Sprintf("%.2f KiB", float64(bytes)/KiB)
	case bytes < GiB:
		return fmt.Sprintf("%.2f MiB", float64(bytes)/MiB)
	case bytes < TiB:
		return fmt.Sprintf("%.2f GiB", float64(bytes)/GiB)
	case bytes < PiB:
		return fmt.Sprintf("%.2f TiB", float64(bytes)/TiB)
	case bytes < EiB:
		return fmt.Sprintf("%.2f PiB", float64(bytes)/PiB)
	default:
		return fmt.Sprintf("%.2f EiB", float64(bytes)/EiB)
	}
}
