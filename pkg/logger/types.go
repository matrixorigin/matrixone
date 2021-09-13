// Copyright 2021 Matrix Origin
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

package logger

import (
	"io"
	"log"
)

type Log interface {
	SetLevel(int)

	SetDepth(int)

	Debug(...interface{})
	Debugn(...interface{})
	Debugf(string, ...interface{})

	Info(...interface{})
	Infon(...interface{})
	Infof(string, ...interface{})

	Warn(...interface{})
	Warnn(...interface{})
	Warnf(string, ...interface{})

	Error(...interface{})
	Errorn(...interface{})
	Errorf(string, ...interface{})

	Fatal(...interface{})
	Fataln(...interface{})
	Fatalf(string, ...interface{})

	Panic(...interface{})
	Panicn(...interface{})
	Panicf(string, ...interface{})
}

const (
	DEBUG = iota
	INFO
	WARN
	ERROR
	FATAL
	PANIC
)

const (
	tagInfo  = "INFO: "
	tagWarn  = "WARN: "
	tagError = "ERROR: "
	tagFatal = "FATAL: "
	tagPanic = "PANIC: "
	tagDebug = "DEBUG: "
)

const (
	flags = log.Ldate | log.Ltime | log.Lshortfile
)

type logger struct {
	level int
	depth int
	wc    io.Writer
	dlog  *log.Logger
	ilog  *log.Logger
	wlog  *log.Logger
	elog  *log.Logger
	flog  *log.Logger
	plog  *log.Logger
}
