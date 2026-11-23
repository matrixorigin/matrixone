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
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// SetupMOLogger sets up the global logger for MO Server.
func SetupMOLogger(conf *LogConfig) {
	setGlobalLogConfig(conf) // keep for dragonboat/v4, ... and so on
	logger, err := initMOLogger(conf)
	if err != nil {
		panic(err)
	}
	replaceGlobalLogger(logger)
	Infof("MO logger init, level=%s, log file=%s, stackLevel=%s", conf.Level, conf.Filename, conf.StacktraceLevel)
}

var loggerTestScopeMu sync.Mutex

type loggerTestState struct {
	config            *LogConfig
	logger, skip, err *zap.Logger
}

func loadLoggerTestState() loggerTestState {
	return loggerTestState{
		config: getGlobalLogConfig(), logger: GetGlobalLogger(),
		skip: GetSkip1Logger(), err: GetErrorLogger(),
	}
}

func storeLoggerTestState(state loggerTestState) {
	gLogConfigs.Store(state.config)
	_globalLogger.Store(state.logger)
	_skip1Logger.Store(state.skip)
	_errorLogger.Store(state.err)
}

// SetupMOLoggerForTest temporarily configures the process-global logger.
// The temporary logger must use the console; this scope does not own file sinks.
// The caller must restore it on every exit path and have exclusive ownership
// of logger configuration: neither this scope nor other work may concurrently
// call SetupMOLogger, mutate a LogConfig in place, or start another test scope.
// Concurrent readers can see the temporary logger. The idempotent restore
// reports false if it observes different config/logger identities; it does not
// promise to detect misuse racing with the restore itself.
func SetupMOLoggerForTest(conf *LogConfig) (restore func() bool) {
	if conf == nil || (conf.Filename != "" && conf.Filename != "console") {
		panic("logutil: test logger scope requires console output")
	}
	if !loggerTestScopeMu.TryLock() {
		panic("logutil: overlapping test logger scopes")
	}
	previous := loadLoggerTestState()
	returned := false
	defer func() {
		if !returned {
			if restore != nil {
				restore()
			} else {
				storeLoggerTestState(previous)
				loggerTestScopeMu.Unlock()
			}
		}
	}()

	// Keep SetupMOLogger's installation sequence, but record ownership before
	// logging: a reporter callback can itself reconfigure the global logger.
	setGlobalLogConfig(conf)
	logger, err := initMOLogger(conf)
	if err != nil {
		panic(err)
	}
	replaceGlobalLogger(logger)
	installed := loadLoggerTestState()
	var once sync.Once
	var unchanged bool
	restore = func() bool {
		once.Do(func() {
			defer loggerTestScopeMu.Unlock()
			current := loadLoggerTestState()
			unchanged = current == installed
			// A failed SetupMOLogger can replace only the config before it
			// panics. Restore if all logger objects still belong to this scope.
			if current.logger == installed.logger && current.skip == installed.skip && current.err == installed.err {
				storeLoggerTestState(previous)
			}
		})
		return unchanged
	}
	Infof("MO logger init, level=%s, log file=%s, stackLevel=%s", conf.Level, conf.Filename, conf.StacktraceLevel)
	if loadLoggerTestState() != installed {
		panic("logutil: logger reconfigured during test scope setup")
	}
	returned = true
	return restore
}

// NewMOLogger new a mo zap logger
func NewMOLogger(cfg *LogConfig) (*zap.Logger, error) {
	return GetLoggerWithOptions(cfg.GetLevel(), cfg.getEncoder(), cfg.getSyncer(), cfg.getOptions()...), nil
}

// initMOLogger initializes a zap Logger.
func initMOLogger(cfg *LogConfig) (*zap.Logger, error) {
	return GetLoggerWithOptions(cfg.GetLevel(), cfg.getEncoder(), cfg.getSyncer(), cfg.getOptions()...), nil
}

// global zap logger for MO server.
var _globalLogger atomic.Value
var _skip1Logger atomic.Value
var _errorLogger atomic.Value

// init initializes a default zap logger before set up logger.
func init() {
	SetLogReporter(&TraceReporter{noopReportZap, noopContextField})
	conf := GetDefaultConfig()
	setGlobalLogConfig(&conf)
	logger, _ := initMOLogger(&conf)
	replaceGlobalLogger(logger)
}

func GetDefaultConfig() LogConfig {
	return LogConfig{Level: "info", Format: "console", StacktraceLevel: "panic"}
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
	_errorLogger.Store(logger.WithOptions(zap.AddCallerSkip(1)))
}

type LogConfig struct {
	Level      string `toml:"level" user_setting:"basic"`
	Format     string `toml:"format" user_setting:"basic"`
	Filename   string `toml:"filename" user_setting:"advanced"`
	MaxSize    int    `toml:"max-size"`
	MaxDays    int    `toml:"max-days"`
	MaxBackups int    `toml:"max-backups"`
	// DisableStore ctrl store log into db
	DisableStore bool `toml:"disable-store"`
	// DisableLog ctrl log into console
	DisableLog bool `toml:"disable-log"`
	// StacktraceLevel
	StacktraceLevel string `toml:"stacktrace-level"`
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

func (cfg *LogConfig) GetLevel() zap.AtomicLevel {
	level := zap.NewAtomicLevel()
	err := level.UnmarshalText([]byte(cfg.Level))
	if err != nil {
		panic(err)
	}
	return level
}

// getStacktraceLevel return stacktrace level.
// If cfg.StacktraceLevel == "", then return zap.PanicLevel
func (cfg *LogConfig) getStacktraceLevel() (level zapcore.Level) {
	if len(cfg.StacktraceLevel) == 0 {
		return zap.PanicLevel
	}
	err := level.UnmarshalText([]byte(cfg.StacktraceLevel))
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
	return []zap.Option{zap.AddStacktrace(cfg.getStacktraceLevel()), zap.AddCaller()}
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
		panic(moerr.NewInternalErrorf(context.Background(), "unsupported log format: %s", format))
	}
}

func getConsoleSyncer() zapcore.WriteSyncer {
	syncer, _, err := zap.Open([]string{"stdout"}...)
	if err != nil {
		panic(err)
	}
	return syncer
}
