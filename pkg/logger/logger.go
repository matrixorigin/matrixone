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
	if l.level >= DEBUG {
		l.dlog.Output(l.depth, fmt.Sprint(v...))
	}
}

func (l *logger) Debugn(v ...interface{}) {
	if l.level >= DEBUG {
		l.dlog.Output(l.depth, fmt.Sprintln(v...))
	}
}

func (l *logger) Debugf(format string, v ...interface{}) {
	if l.level >= DEBUG {
		l.dlog.Output(l.depth, fmt.Sprintf(format, v...))
	}
}

func (l *logger) Info(v ...interface{}) {
	if l.level >= INFO {
		l.ilog.Output(l.depth, fmt.Sprint(v...))
	}
}

func (l *logger) Infon(v ...interface{}) {
	if l.level >= INFO {
		l.ilog.Output(l.depth, fmt.Sprintln(v...))
	}
}

func (l *logger) Infof(format string, v ...interface{}) {
	if l.level >= INFO {
		l.ilog.Output(l.depth, fmt.Sprintf(format, v...))
	}
}

func (l *logger) Warn(v ...interface{}) {
	if l.level >= WARN {
		l.wlog.Output(l.depth, fmt.Sprint(v...))
	}
}

func (l *logger) Warnn(v ...interface{}) {
	if l.level >= WARN {
		l.wlog.Output(l.depth, fmt.Sprintln(v...))
	}
}

func (l *logger) Warnf(format string, v ...interface{}) {
	if l.level >= WARN {
		l.wlog.Output(l.depth, fmt.Sprintf(format, v...))
	}
}

func (l *logger) Error(v ...interface{}) {
	if l.level >= ERROR {
		l.elog.Output(l.depth, fmt.Sprint(v...))
	}
}

func (l *logger) Errorn(v ...interface{}) {
	if l.level >= ERROR {
		l.elog.Output(l.depth, fmt.Sprintln(v...))
	}
}

func (l *logger) Errorf(format string, v ...interface{}) {
	if l.level >= ERROR {
		l.elog.Output(l.depth, fmt.Sprintf(format, v...))
	}
}

func (l *logger) Fatal(v ...interface{}) {
	if l.level >= FATAL {
		l.flog.Output(l.depth, fmt.Sprint(v...))
		os.Exit(1)
	}
}

func (l *logger) Fataln(v ...interface{}) {
	if l.level >= FATAL {
		l.flog.Output(l.depth, fmt.Sprintln(v...))
		os.Exit(1)
	}
}

func (l *logger) Fatalf(format string, v ...interface{}) {
	if l.level >= FATAL {
		l.flog.Output(l.depth, fmt.Sprintf(format, v...))
		os.Exit(1)
	}
}

func (l *logger) Panic(v ...interface{}) {
	if l.level >= PANIC {
		l.plog.Output(l.depth, fmt.Sprint(v...))
		panic(v)
	}
}

func (l *logger) Panicn(v ...interface{}) {
	if l.level >= PANIC {
		l.plog.Output(l.depth, fmt.Sprintln(v...))
		panic(v)
	}
}

func (l *logger) Panicf(format string, v ...interface{}) {
	if l.level >= PANIC {
		l.plog.Output(l.depth, fmt.Sprintf(format, v...))
		panic(v)
	}
}
