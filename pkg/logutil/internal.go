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
	"context"
	"os"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// SetupMOLogger sets up the global logger for MO Server.
func SetupMOLogger(conf *LogConfig) {
	setGlobalLogConfig(conf) // keep for dragonboat/v4, ... and so on
	logger, err := initMOLogger(conf)
	if err != nil {
		panic(err)
	}
	replaceGlobalLogger(logger)
	Infof("MO logger init, level=%s, log file=%s", conf.Level, conf.Filename)
}

// initMOLogger initializes a zap Logger.
func initMOLogger(cfg *LogConfig) (*zap.Logger, error) {
	return GetLoggerWithOptions(cfg.getLevel(), cfg.getEncoder(), cfg.getSyncer()), nil
}

// global zap logger for MO server.
var _globalLogger atomic.Value
var _skip1Logger atomic.Value
var _errorLogger atomic.Value

// init initializes a default zap logger before set up logger.
func init() {
	SetLogReporter(&TraceReporter{noopReportZap, noopContextField})
	conf := &LogConfig{Level: "info", Format: "console"}
	setGlobalLogConfig(conf)
	logger, _ := initMOLogger(conf)
	replaceGlobalLogger(logger)
}

// GetGlobalLogger returns the current global zap Logger.
func GetGlobalLogger() *zap.Logger {
	return _globalLogger.Load().(*zap.Logger)
}

func GetSkip1Logger() *zap.Logger {
	return _skip1Logger.Load().(*zap.Logger)
}

func GetErrorLogger() *zap.Logger {
	return _errorLogger.Load().(*zap.Logger)
}

// replaceGlobalLogger replaces the current global zap Logger.
func replaceGlobalLogger(logger *zap.Logger) {
	_globalLogger.Store(logger)
	_skip1Logger.Store(logger.WithOptions(zap.AddCallerSkip(1)))
	_errorLogger.Store(logger.WithOptions(zap.AddCallerSkip(1), zap.AddStacktrace(zap.ErrorLevel)))
}

type LogConfig struct {
	Level      string `toml:"level"`
	Format     string `toml:"format"`
	Filename   string `toml:"filename"`
	MaxSize    int    `toml:"max-size"`
	MaxDays    int    `toml:"max-days"`
	MaxBackups int    `toml:"max-backups"`
	// DisableStore ctrl store log into db
	DisableStore bool `toml:"disable-store"`
	// DisableLog ctrl log into console
	DisableLog bool `toml:"disable-log"`
}

func (cfg *LogConfig) getSyncer() zapcore.WriteSyncer {
	if cfg.Filename == "" || cfg.Filename == "console" {
		return getConsoleSyncer()
	}

	if stat, err := os.Stat(cfg.Filename); err == nil {
		if stat.IsDir() {
			panic("log file can't be a directory")
		}
	}

	if cfg.MaxSize == 0 {
		cfg.MaxSize = 512
	}
	// add lumberjack logger
	return zapcore.AddSync(&lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxAge:     cfg.MaxDays,
		MaxBackups: cfg.MaxBackups,
		LocalTime:  true,
		Compress:   false,
	})
}

func (cfg *LogConfig) getEncoder() zapcore.Encoder {
	return getLoggerEncoder(cfg.Format)
}

func (cfg *LogConfig) getLevel() zap.AtomicLevel {
	level := zap.NewAtomicLevel()
	err := level.UnmarshalText([]byte(cfg.Level))
	if err != nil {
		panic(err)
	}
	return level
}

func (cfg *LogConfig) getSinks() (sinks []ZapSink) {
	if !cfg.DisableLog {
		encoder, syncer := cfg.getEncoder(), cfg.getSyncer()
		sinks = append(sinks, ZapSink{encoder, syncer})
	}
	if !cfg.DisableStore {
		encoder, syncer := getTraceLogSinks()
		sinks = append(sinks, ZapSink{encoder, syncer})
	}
	return
}

func (cfg *LogConfig) getOptions() []zap.Option {
	return []zap.Option{zap.AddStacktrace(zapcore.FatalLevel), zap.AddCaller()}
}

func getLoggerEncoder(format string) zapcore.Encoder {
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

	switch format {
	case "json", "":
		return zapcore.NewJSONEncoder(encoderConfig)
	case "console":
		return zapcore.NewConsoleEncoder(encoderConfig)
	default:
		panic(moerr.NewInternalError(context.Background(), "unsupported log format: %s", format))
	}
}

func getConsoleSyncer() zapcore.WriteSyncer {
	syncer, _, err := zap.Open([]string{"stdout"}...)
	if err != nil {
		panic(err)
	}
	return syncer
}
