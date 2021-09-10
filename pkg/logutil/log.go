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
	"fmt"
	"github.com/BurntSushi/toml"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"sync/atomic"
	"time"
)

var _globalLogger atomic.Value

// init a default zap logger before set up logger.
func init() {
	conf := &Config{Level: "info", File: FileLogConfig{}}
	logger, _ := InitLogger(conf)
	ReplaceGlobalLogger(logger)
}

// SetupLogger sets up the global logger for MO Server
func SetupLogger(configFile string) {
	var conf Config
	if _, err := toml.DecodeFile(configFile, &conf); err != nil {
		panic(err)
	}
	logger, err := InitLogger(&conf)
	if err != nil {
		panic(err)
	}
	ReplaceGlobalLogger(logger)
	Debugf("db logger init, level=%s, log file=%s", conf.Level, conf.File.Filename)
}

// InitLogger initializes a zap Logger.
func InitLogger(cfg *Config) (*zap.Logger, error) {
	var syncer zapcore.WriteSyncer

	// add output for zap logger.
	if len(cfg.File.Filename) > 0 {
		ll, err := getLL(&cfg.File)
		if err != nil {
			return nil, err
		}
		syncer = zapcore.AddSync(ll)
	} else {
		stdOut, _, err := zap.Open([]string{"stdout"}...)
		if err != nil {
			return nil, err
		}
		syncer = stdOut
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
	core := zapcore.NewCore(NewZapEncoder(cfg), syncer, level)
	logger := zap.New(core)
	return logger, nil
}

// NewZapEncoder creates a json/console zap Encoder.
func NewZapEncoder(cfg *Config) zapcore.Encoder {
	cc := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "name",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.TimeEncoder(func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.UTC().Format("2006-01-02T15:04:05Z0700"))
		}),
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.FullCallerEncoder,
	}
	switch cfg.Format {
	case "json", "":
		return zapcore.NewJSONEncoder(cc)
	case "console":
		return zapcore.NewConsoleEncoder(cc)
	default:
		panic(fmt.Sprintf("unsupport log format: %s", cfg.Format))
	}
}

// getLL returns a lumberjack Logger based on file log config.
func getLL(cfg *FileLogConfig) (*lumberjack.Logger, error) {
	if stat, err := os.Stat(cfg.Filename); err == nil {
		if stat.IsDir() {
			return nil, errors.New("log file can't be a directory")
		}
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = defaultLogMaxSize
	}
	return &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxDays,
		LocalTime:  true,
	}, nil
}

// GetGlobalLogger returns the current global zap Logger.
func GetGlobalLogger() *zap.Logger {
	return _globalLogger.Load().(*zap.Logger)
}

// ReplaceGlobalLogger replaces the current global zap Logger.
func ReplaceGlobalLogger(logger *zap.Logger) {
	_globalLogger.Store(logger)
}
