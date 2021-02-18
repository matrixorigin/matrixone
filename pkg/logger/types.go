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
