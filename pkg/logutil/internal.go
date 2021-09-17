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

package logutil

import (
	"errors"
	"github.com/BurntSushi/toml"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"sync/atomic"
	"time"
)

// SetupMOLogger sets up the global logger for MO Server.
func SetupMOLogger(path string) {
	var conf loggerConfig
	if _, err := toml.DecodeFile(path, &conf); err != nil {
		panic(err)
	}
	logger, err := initMOLogger(&conf)
	if err != nil {
		panic(err)
	}
	replaceGlobalLogger(logger)
	Debugf("MO logger init, level=%s, log file=%s", conf.Level, conf.Filename)
}

// initMOLogger initializes a zap Logger.
func initMOLogger(cfg *loggerConfig) (*zap.Logger, error) {
	var syncer zapcore.WriteSyncer

	// add output for zap logger.
	if len(cfg.Filename) > 0 {
		// has log file
		if stat, err := os.Stat(cfg.Filename); err == nil {
			if stat.IsDir() {
				return nil, errors.New("log file can't be a directory")
			}
		}
		if cfg.MaxSize == 0 {
			cfg.MaxSize = 512
		}
		// add lumberjack logger
		syncer = zapcore.AddSync(&lumberjack.Logger{
			Filename:   cfg.Filename,
			MaxSize:    cfg.MaxSize,
			MaxAge:     cfg.MaxDays,
			MaxBackups: cfg.MaxBackups,
			LocalTime:  true,
			Compress:   false,
		})
	} else {
		// output to stdout directly
		var err error
		syncer, _, err = zap.Open([]string{"stdout"}...)
		if err != nil {
			return nil, err
		}
	}

	level := zap.NewAtomicLevel()
	// don't handle change level request currently, cause tests would not
	// pass if running together (panic: listen tcp :9090: bind: address already in use)
	//
	// handleLevelChange(port, pattern, level)
	err := level.UnmarshalText([]byte(cfg.Level))
	if err != nil {
		return nil, err
	}

	var encoder zapcore.Encoder
	encoderConfig := zapcore.EncoderConfig{
		MessageKey:    "msg",
		LevelKey:      "level",
		TimeKey:       "time",
		NameKey:       "name",
		CallerKey:     "caller",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.CapitalLevelEncoder,
		EncodeTime: zapcore.TimeEncoder(func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006/01/02 15:04:05.000000 -0700"))
		}),
		EncodeDuration:   zapcore.StringDurationEncoder,
		EncodeCaller:     zapcore.ShortCallerEncoder,
		ConsoleSeparator: " ",
	}

	switch cfg.Format {
	case "json", "":
		// default format is json
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	case "console":
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	default:
		return nil, errors.New("unsupported log format")
	}

	return zap.New(zapcore.NewCore(encoder, syncer, level), zap.AddCaller()), nil
}

// global zap logger for MO server.
var _globalLogger atomic.Value

// init initializes a default zap logger before set up logger.
func init() {
	conf := &loggerConfig{Level: "info", Format: "console"}
	logger, _ := initMOLogger(conf)
	replaceGlobalLogger(logger)
}

// GetGlobalLogger returns the current global zap Logger.
func GetGlobalLogger() *zap.Logger {
	return _globalLogger.Load().(*zap.Logger)
}

// replaceGlobalLogger replaces the current global zap Logger.
func replaceGlobalLogger(logger *zap.Logger) {
	_globalLogger.Store(logger)
}

type loggerConfig struct {
	Level      string `toml:"level"`
	Format     string `toml:"format"`
	Filename   string `toml:"filename"`
	MaxSize    int    `toml:"max-size"`
	MaxDays    int    `toml:"max-days"`
	MaxBackups int    `toml:"max-backups"`
}
