// Copyright 2021 - 2026 MatrixOne
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package aggexec

import (
	"encoding/binary"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// The ewm window-statistics configuration channel (issue #29945, §9):
// constructEwmConfig evaluates the decay and policy constants against the
// current compilation's proc and encodes the resolved configuration ONCE
// into MakeAggFunctionExpression's extraConfig bytes; the window
// operator's EWM dispatch reads its own expression's bytes and decodes
// them once at container setup. The bytes are immutable, shared inputs
// across operator copies and carry no execution state.

// ewmConfigVersion is the single on-wire format version. A decode of any
// other version fails closed with a construction error.
const ewmConfigVersion byte = 1

// Ewm decay modes carried by the configuration's Mode byte.
const (
	// EwmModeRow is the numeric-alpha row-decay form.
	EwmModeRow byte = 0
	// EwmModeTime is the 'halflife' event-time form.
	EwmModeTime byte = 1
)

// EwmConfig is the fully resolved per-execution configuration of one
// ewm window-statistics call.
type EwmConfig struct {
	Mode     uint8 // EwmModeRow / EwmModeTime
	Alpha    float64
	HalfLife float64
	Adjust   bool
	IgnoreNA bool
	// MinPeriods carries any nonnegative integer threshold (§4 declares
	// no upper limit), not just the int32 axis.
	MinPeriods int64
}

// EncodeEwmConfig packs the resolved configuration into the config
// bytes (one versioned little-endian record).
func EncodeEwmConfig(cfg EwmConfig) []byte {
	buf := make([]byte, 1+1+1+1+8+8+8)
	i := 0
	buf[i] = ewmConfigVersion
	i++
	buf[i] = cfg.Mode
	i++
	if cfg.Adjust {
		buf[i] = 1
	}
	i++
	if cfg.IgnoreNA {
		buf[i] = 1
	}
	i++
	binary.LittleEndian.PutUint64(buf[i:], math.Float64bits(cfg.Alpha))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], math.Float64bits(cfg.HalfLife))
	i += 8
	binary.LittleEndian.PutUint64(buf[i:], uint64(cfg.MinPeriods))
	i += 8
	return buf[:i]
}

// DecodeEwmConfig unpacks the configuration bytes. Any truncation,
// unknown version or unknown mode is an error (the bytes are produced
// by EncodeEwmConfig inside the same process version; anything else is
// a construction bug, never user input).
func DecodeEwmConfig(b []byte) (EwmConfig, error) {
	// version + mode + adjust + ignoreNA + alpha + halflife + minPeriods
	if len(b) != 1+1+1+1+8+8+8 {
		return EwmConfig{}, moerr.NewInternalErrorNoCtx(
			"invalid ewm config bytes: unexpected length")
	}
	if b[0] != ewmConfigVersion {
		return EwmConfig{}, moerr.NewInternalErrorNoCtx(
			"invalid ewm config bytes: unsupported version")
	}
	if b[1] != EwmModeRow && b[1] != EwmModeTime {
		return EwmConfig{}, moerr.NewInternalErrorNoCtx(
			"invalid ewm config bytes: unknown decay mode")
	}
	return EwmConfig{
		Mode:       b[1],
		Adjust:     b[2] != 0,
		IgnoreNA:   b[3] != 0,
		Alpha:      math.Float64frombits(binary.LittleEndian.Uint64(b[4:])),
		HalfLife:   math.Float64frombits(binary.LittleEndian.Uint64(b[12:])),
		MinPeriods: int64(binary.LittleEndian.Uint64(b[20:])),
	}, nil
}
