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
	"fmt"
	"io"
	"log"
	"os"
)

func New(wc io.Writer, prefix string) *logger {
	if len(prefix) == 0 {
		return &logger{
			wc:    wc,
			depth: 2,
			level: INFO,
			dlog:  log.New(wc, tagDebug, flags),
			ilog:  log.New(wc, tagInfo, flags),
			wlog:  log.New(wc, tagWarn, flags),
			elog:  log.New(wc, tagError, flags),
			flog:  log.New(wc, tagFatal, flags),
			plog:  log.New(wc, tagPanic, flags),
		}
	} else {
		return &logger{
			wc:    wc,
			depth: 2,
			level: INFO,
			dlog:  log.New(wc, prefix+" "+tagDebug, flags),
			ilog:  log.New(wc, prefix+" "+tagInfo, flags),
			wlog:  log.New(wc, prefix+" "+tagWarn, flags),
			elog:  log.New(wc, prefix+" "+tagError, flags),
			flog:  log.New(wc, prefix+" "+tagFatal, flags),
			plog:  log.New(wc, prefix+" "+tagPanic, flags),
		}
	}
}

func (l *logger) SetLevel(level int) {
	l.level = level
}

func (l *logger) SetDepth(depth int) {
	l.depth = depth
}

func (l *logger) Debug(v ...interface{}) {
	if l.level <= DEBUG {
		if err := l.dlog.Output(l.depth, fmt.Sprint(v...)); err != nil {
			panic(err)
		}
	}
}

func (l *logger) Debugn(v ...interface{}) {
	if l.level <= DEBUG {
		if err := l.dlog.Output(l.depth, fmt.Sprintln(v...)); err != nil {
			panic(err)
		}
	}
}

func (l *logger) Debugf(format string, v ...interface{}) {
	if l.level <= DEBUG {
		if err := l.dlog.Output(l.depth, fmt.Sprintf(format, v...)); err != nil {
			panic(err)
		}
	}
}

func (l *logger) Info(v ...interface{}) {
	if l.level <= INFO {
		if err := l.ilog.Output(l.depth, fmt.Sprint(v...)); err != nil {
			panic(err)
		}
	}
}

func (l *logger) Infon(v ...interface{}) {
	if l.level <= INFO {
		if err := l.ilog.Output(l.depth, fmt.Sprintln(v...)); err != nil {
			panic(err)
		}
	}
}

func (l *logger) Infof(format string, v ...interface{}) {
	if l.level <= INFO {
		if err := l.ilog.Output(l.depth, fmt.Sprintf(format, v...)); err != nil {
			panic(err)
		}
	}
}

func (l *logger) Warn(v ...interface{}) {
	if l.level <= WARN {
		if err := l.wlog.Output(l.depth, fmt.Sprint(v...)); err != nil {
			panic(err)
		}
	}
}

func (l *logger) Warnn(v ...interface{}) {
	if l.level <= WARN {
		if err := l.wlog.Output(l.depth, fmt.Sprintln(v...)); err != nil {
			panic(err)
		}
	}
}

func (l *logger) Warnf(format string, v ...interface{}) {
	if l.level <= WARN {
		if err := l.wlog.Output(l.depth, fmt.Sprintf(format, v...)); err != nil {
			panic(err)
		}
	}
}

func (l *logger) Error(v ...interface{}) {
	if l.level <= ERROR {
		if err := l.elog.Output(l.depth, fmt.Sprint(v...)); err != nil {
			panic(err)
		}
	}
}

func (l *logger) Errorn(v ...interface{}) {
	if l.level <= ERROR {
		if err := l.elog.Output(l.depth, fmt.Sprintln(v...)); err != nil {
			panic(err)
		}
	}
}

func (l *logger) Errorf(format string, v ...interface{}) {
	if l.level <= ERROR {
		if err := l.elog.Output(l.depth, fmt.Sprintf(format, v...)); err != nil {
			panic(err)
		}
	}
}

func (l *logger) Fatal(v ...interface{}) {
	if l.level <= FATAL {
		if err := l.flog.Output(l.depth, fmt.Sprint(v...)); err != nil {
			panic(err)
		}
		os.Exit(1)
	}
}

func (l *logger) Fataln(v ...interface{}) {
	if l.level <= FATAL {
		if err := l.flog.Output(l.depth, fmt.Sprintln(v...)); err != nil {
			panic(err)
		}
		os.Exit(1)
	}
}

func (l *logger) Fatalf(format string, v ...interface{}) {
	if l.level <= FATAL {
		if err := l.flog.Output(l.depth, fmt.Sprintf(format, v...)); err != nil {
			panic(err)
		}
		os.Exit(1)
	}
}

func (l *logger) Panic(v ...interface{}) {
	if l.level <= PANIC {
		if err := l.plog.Output(l.depth, fmt.Sprint(v...)); err != nil {
			panic(err)
		}
		panic(v)
	}
}

func (l *logger) Panicn(v ...interface{}) {
	if l.level <= PANIC {
		if err := l.plog.Output(l.depth, fmt.Sprintln(v...)); err != nil {
			panic(err)
		}
		panic(v)
	}
}

func (l *logger) Panicf(format string, v ...interface{}) {
	if l.level <= PANIC {
		if err := l.plog.Output(l.depth, fmt.Sprintf(format, v...)); err != nil {
			panic(err)
		}
		panic(v)
	}
}
