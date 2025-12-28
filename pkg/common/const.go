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

// FormatBytes formats bytes to human-readable string using decimal units (B, KB, MB, GB, TB)
// Examples: 0 -> "0B", 1386624 -> "1.39MB", 1024 -> "1.02KB"
// This uses decimal units (1000-based) instead of 1024-based)
func FormatBytes(bytes int64) string {
	if bytes == 0 {
		return "0B"
	}
	num := float64(bytes)
	if bytes < THOUSAND {
		return fmt.Sprintf("%dB", bytes)
	}
	if bytes < MILLION {
		return fmt.Sprintf("%.2fKB", num/THOUSAND)
	}
	if bytes < BILLION {
		return fmt.Sprintf("%.2fMB", num/MILLION)
	}
	if bytes < TRILLION {
		return fmt.Sprintf("%.2fGB", num/BILLION)
	}
	return fmt.Sprintf("%.2fTB", num/TRILLION)
}

// FormatDuration formats nanoseconds to human-readable string (ms or s)
// Examples: 0 -> "0ms", 21625539 -> "21.63ms", 1000000000 -> "1.00s"
func FormatDuration(ns int64) string {
	if ns == 0 {
		return "0ns"
	}
	const (
		nanosPerMilli = 1000000
		nanosPerSec   = 1000000000
	)
	if ns < nanosPerSec {
		return fmt.Sprintf("%.2fms", float64(ns)/nanosPerMilli)
	}
	return fmt.Sprintf("%.2fs", float64(ns)/nanosPerSec)
}
