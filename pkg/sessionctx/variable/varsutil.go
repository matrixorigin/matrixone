// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

// GetSessionSystemVar gets a system variable.
// If it is a session only variable, use the default value defined in code.
// Returns error if there is no such variable.
func GetSessionSystemVar(s *SessionVars, key string) (string, error) {
	key = strings.ToLower(key)
	gVal, ok, err := GetSessionOnlySysVars(s, key)
	if err != nil || ok {
		return gVal, err
	}
	gVal, err = s.GlobalVarsAccessor.GetGlobalSysVar(key)
	if err != nil {
		return "", err
	}
	s.systems[key] = gVal
	return gVal, nil
}

// GetSessionOnlySysVars get the default value defined in code for session only variable.
// The return bool value indicates whether it's a session only variable.
func GetSessionOnlySysVars(s *SessionVars, key string) (string, bool, error) {
	return "", false, ErrUnknownSystemVar.GenWithStackByArgs(key)
}

// GetGlobalSystemVar gets a global system variable.
func GetGlobalSystemVar(s *SessionVars, key string) (string, error) {
	key = strings.ToLower(key)
	gVal, ok, err := GetScopeNoneSystemVar(key)
	if err != nil || ok {
		return gVal, err
	}
	gVal, err = s.GlobalVarsAccessor.GetGlobalSysVar(key)
	if err != nil {
		return "", err
	}
	return gVal, nil
}

// GetScopeNoneSystemVar checks the validation of `key`,
// and return the default value if its scope is `ScopeNone`.
func GetScopeNoneSystemVar(key string) (string, bool, error) {
	return "", false, ErrUnknownSystemVar.GenWithStackByArgs(key)
}

// epochShiftBits is used to reserve logical part of the timestamp.
const epochShiftBits = 18

// ValidateGetSystemVar checks if system variable exists and validates its scope when get system variable.
func ValidateGetSystemVar(name string, isGlobal bool) error {
	return ErrUnknownSystemVar.GenWithStackByArgs(name)
}

const (
	// initChunkSizeUpperBound indicates upper bound value of tidb_init_chunk_size.
	initChunkSizeUpperBound = 32
	// maxChunkSizeLowerBound indicates lower bound value of tidb_max_chunk_size.
	maxChunkSizeLowerBound = 32
)

// ValidateSetSystemVar checks if system variable satisfies specific restriction.
// func ValidateSetSystemVar(vars *SessionVars, name string, value string, scope ScopeFlag) (string, error) {
// 	return value, ErrUnknownSystemVar.GenWithStackByArgs(name)
// }

// TiDBOptOn could be used for all tidb session variable options, we use "ON"/1 to turn on those options.
func DBOptOn(opt string) bool {
	return strings.EqualFold(opt, "ON") || opt == "1"
}

const (
	// OffInt is used by TiDBMultiStatementMode
	OffInt = 0
	// OnInt is used TiDBMultiStatementMode
	OnInt = 1
	// WarnInt is used by TiDBMultiStatementMode
	WarnInt = 2
)

// TiDBOptMultiStmt converts multi-stmt options to int.
func TiDBOptMultiStmt(opt string) int {
	switch opt {
	case BoolOff:
		return OffInt
	case BoolOn:
		return OnInt
	}
	return WarnInt
}

func dbOptPositiveInt32(opt string, defaultVal int) int {
	val, err := strconv.Atoi(opt)
	if err != nil || val <= 0 {
		return defaultVal
	}
	return val
}

func tidbOptInt64(opt string, defaultVal int64) int64 {
	val, err := strconv.ParseInt(opt, 10, 64)
	if err != nil {
		return defaultVal
	}
	return val
}

func tidbOptFloat64(opt string, defaultVal float64) float64 {
	val, err := strconv.ParseFloat(opt, 64)
	if err != nil {
		return defaultVal
	}
	return val
}

// GoTimeToTS converts a Go time to uint64 timestamp.
func GoTimeToTS(t time.Time) uint64 {
	ts := (t.UnixNano() / int64(time.Millisecond)) << epochShiftBits
	return uint64(ts)
}

// serverGlobalVariable is used to handle variables that acts in server and global scope.
type serverGlobalVariable struct {
	sync.Mutex
	serverVal string
	globalVal string
}

// Set sets the value according to variable scope.
func (v *serverGlobalVariable) Set(val string, isServer bool) {
	v.Lock()
	if isServer {
		v.serverVal = val
	} else {
		v.globalVal = val
	}
	v.Unlock()
}

// GetVal gets the value.
func (v *serverGlobalVariable) GetVal() string {
	v.Lock()
	defer v.Unlock()
	if v.serverVal != "" {
		return v.serverVal
	}
	return v.globalVal
}
