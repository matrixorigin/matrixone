// Copyright 2022 Matrix Origin
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

package logutil

import (
	"github.com/lni/dragonboat/v4/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _ logger.ILogger = (*DragonboatAdaptLogger)(nil)

type DragonboatAdaptLogger struct {
	logger  *zap.SugaredLogger
	atom    *zap.AtomicLevel
	pkgName string
}

func (d *DragonboatAdaptLogger) SetLevel(level logger.LogLevel) {
	switch level {
	case logger.CRITICAL:
		d.atom.SetLevel(zapcore.PanicLevel)
	case logger.ERROR:
		d.atom.SetLevel(zapcore.ErrorLevel)
	case logger.WARNING:
		d.atom.SetLevel(zapcore.WarnLevel)
	case logger.INFO:
		d.atom.SetLevel(zapcore.InfoLevel)
	case logger.DEBUG:
		d.atom.SetLevel(zapcore.DebugLevel)
	default:
		d.atom.SetLevel(zapcore.DebugLevel)
	}
}

func (d DragonboatAdaptLogger) Debugf(format string, args ...interface{}) {
	d.logger.Debugf(format, args...)
}

func (d DragonboatAdaptLogger) Infof(format string, args ...interface{}) {
	d.logger.Infof(format, args...)
}

func (d DragonboatAdaptLogger) Warningf(format string, args ...interface{}) {
	d.logger.Warnf(format, args...)
}

func (d DragonboatAdaptLogger) Errorf(format string, args ...interface{}) {
	d.logger.Errorf(format, args...)
}

func (d DragonboatAdaptLogger) Panicf(format string, args ...interface{}) {
	d.logger.Panicf(format, args...)
}

// DragonboatFactory implement logger.Factory for logger.SetLoggerFactory
// create DragonboatAdaptLogger intance
func DragonboatFactory(name string) logger.ILogger {
	var cores = make([]zapcore.Core, 0, 2)
	cfg := getGlobalLogConfig()
	atom := cfg.getLevel()
	sinks := cfg.getSinks()
	for _, sink := range sinks {
		cores = append(cores, zapcore.NewCore(sink.enc, sink.out, atom))
	}
	options := cfg.getOptions()
	options = append(options, zap.AddCallerSkip(2), zap.AddStacktrace(zap.ErrorLevel))
	return &DragonboatAdaptLogger{
		logger:  zap.New(zapcore.NewTee(cores...), options...).Named(name).Sugar(),
		atom:    &atom,
		pkgName: name,
	}
}

func init() {
	logger.SetLoggerFactory(DragonboatFactory)
}
