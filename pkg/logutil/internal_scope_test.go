// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"context"
	"flag"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

type testLoggerState struct {
	config            *LogConfig
	logger, skip, err *zap.Logger
}

func getTestLoggerState() testLoggerState {
	return testLoggerState{
		config: getGlobalLogConfig(), logger: GetGlobalLogger(),
		skip: GetSkip1Logger(), err: GetErrorLogger(),
	}
}

func restoreTestLoggerState(state testLoggerState) {
	gLogConfigs.Store(state.config)
	_globalLogger.Store(state.logger)
	_skip1Logger.Store(state.skip)
	_errorLogger.Store(state.err)
}

func requireTestLoggerState(t *testing.T, want testLoggerState) {
	t.Helper()
	got := getTestLoggerState()
	require.Same(t, want.config, got.config)
	require.Same(t, want.logger, got.logger)
	require.Same(t, want.skip, got.skip)
	require.Same(t, want.err, got.err)
}

func TestSetupMOLoggerForTestRestoresState(t *testing.T) {
	original := getTestLoggerState()
	t.Cleanup(func() { restoreTestLoggerState(original) })
	custom := &LogConfig{Level: "warn", Format: "console", DisableStore: true}
	SetupMOLogger(custom)
	before := getTestLoggerState()

	t.Run("normal and repeated restore", func(t *testing.T) {
		restore := SetupMOLoggerForTest(&LogConfig{Level: "debug", DisableStore: true})
		t.Cleanup(func() { restore() })
		require.True(t, GetSkip1Logger().Core().Enabled(zapcore.DebugLevel))
		require.True(t, restore())
		require.True(t, restore(), "repeated restore must report the same result")
		requireTestLoggerState(t, before)
	})

	t.Run("panic in scope", func(t *testing.T) {
		require.Panics(t, func() {
			restore := SetupMOLoggerForTest(&LogConfig{Level: "debug", DisableStore: true})
			defer restore()
			panic("scope failure")
		})
		requireTestLoggerState(t, before)
	})

	t.Run("setup panic", func(t *testing.T) {
		require.Panics(t, func() {
			SetupMOLoggerForTest(&LogConfig{Level: "not-a-level", DisableStore: true})
		})
		requireTestLoggerState(t, before)
	})

	t.Run("caller DEBUG survives a scoped change", func(t *testing.T) {
		SetupMOLogger(&LogConfig{Level: "debug", Format: "json", DisableStore: true})
		debugState := getTestLoggerState()
		t.Cleanup(func() { restoreTestLoggerState(before) })
		restore := SetupMOLoggerForTest(&LogConfig{Level: "error", DisableStore: true})
		t.Cleanup(func() { restore() })
		require.True(t, restore())
		requireTestLoggerState(t, debugState)
		require.True(t, GetSkip1Logger().Core().Enabled(zapcore.DebugLevel))
	})

	t.Run("Goexit in scope", func(t *testing.T) {
		done := make(chan struct{})
		go func() {
			defer close(done)
			restore := SetupMOLoggerForTest(&LogConfig{Level: "debug", DisableStore: true})
			defer restore()
			runtime.Goexit()
		}()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatal("logger scope was not restored after Goexit")
		}
		requireTestLoggerState(t, before)
	})
}

func TestSetupMOLoggerDuringTestScope(t *testing.T) {
	original := getTestLoggerState()
	t.Cleanup(func() { restoreTestLoggerState(original) })

	restore := SetupMOLoggerForTest(&LogConfig{Level: "debug", DisableStore: true})
	t.Cleanup(func() { restore() })
	newConfig := &LogConfig{Level: "error", Format: "console", DisableStore: true}
	// A completed sequential reconfiguration is detected; concurrent setup is
	// outside the test scope's exclusive-ownership contract.
	SetupMOLogger(newConfig)
	require.False(t, restore(), "a reconfiguration must be reported")
	require.False(t, restore(), "repeated restore must report the same conflict")
	require.Same(t, newConfig, getGlobalLogConfig(), "restore must not overwrite the newer setup")
}

func TestFailedSetupDuringTestScope(t *testing.T) {
	original := getTestLoggerState()
	t.Cleanup(func() { restoreTestLoggerState(original) })
	restore := SetupMOLoggerForTest(&LogConfig{Level: "debug", DisableStore: true})
	t.Cleanup(func() { restore() })
	installed := getTestLoggerState()

	require.Panics(t, func() { SetupMOLogger(&LogConfig{Level: "not-a-level"}) })
	require.Same(t, installed.logger, GetGlobalLogger())
	require.NotSame(t, installed.config, getGlobalLogConfig())
	require.False(t, restore(), "a failed setup must be reported")
	requireTestLoggerState(t, original)
}

func TestTestLoggerScopeRejectsFileSink(t *testing.T) {
	original := getTestLoggerState()
	for _, conf := range []*LogConfig{nil, {Level: "debug", Filename: "file.log"}} {
		require.PanicsWithValue(t, "logutil: test logger scope requires console output", func() {
			SetupMOLoggerForTest(conf)
		})
		requireTestLoggerState(t, original)
	}
	restore := SetupMOLoggerForTest(&LogConfig{Level: "debug", Filename: "console", DisableStore: true})
	t.Cleanup(func() { restore() })
	require.True(t, restore(), "a rejected scope must not retain its lock")
	requireTestLoggerState(t, original)
}

func TestOverlappingTestLoggerScopesFailFast(t *testing.T) {
	original := getTestLoggerState()
	t.Cleanup(func() { restoreTestLoggerState(original) })
	restore := SetupMOLoggerForTest(&LogConfig{Level: "debug", DisableStore: true})
	t.Cleanup(func() { restore() })
	require.PanicsWithValue(t, "logutil: overlapping test logger scopes", func() {
		SetupMOLoggerForTest(&LogConfig{Level: "warn", DisableStore: true})
	})
	require.True(t, restore())
	requireTestLoggerState(t, original)
}

func runLoggerScopeChild(t *testing.T, testName, childEnv string, childArgs ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	args := []string{"-test.run=^" + testName + "$", "-test.timeout=15s", "-test.v"}
	if f := flag.Lookup("test.gocoverdir"); f != nil && f.Value.String() != "" {
		args = append(args, "-test.gocoverdir="+f.Value.String())
	}
	args = append(args, "--")
	args = append(args, childArgs...)
	cmd := exec.CommandContext(ctx, os.Args[0], args...)
	cmd.WaitDelay = 3 * time.Second
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, childEnv+"=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, childEnv+"=1")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "logger scope child failed: %s", output)
	return string(output)
}

func TestSetupMOLoggerReporterCanReenter(t *testing.T) {
	const childEnv = "MO_LOGUTIL_REENTRY_TEST_CHILD"
	const childArg = "mo-logutil-reentry-helper"
	if os.Getenv(childEnv) == "1" && len(flag.Args()) == 1 && flag.Arg(0) == childArg {
		original := getTestLoggerState()
		previousReporter := GetReportZapFunc()
		t.Cleanup(func() {
			SetLogReporter(&TraceReporter{ReportZap: previousReporter})
			restoreTestLoggerState(original)
		})
		var entered atomic.Bool
		newConfig := &LogConfig{Level: "error", DisableStore: true}
		SetLogReporter(&TraceReporter{ReportZap: func(zapcore.Encoder, zapcore.Entry, []zapcore.Field) (*buffer.Buffer, error) {
			if entered.CompareAndSwap(false, true) {
				SetupMOLogger(newConfig)
			}
			return buffer.NewPool().Get(), nil
		}})
		SetupMOLogger(&LogConfig{Level: "debug"})
		require.True(t, entered.Load(), "the reporter callback must execute")
		require.Same(t, newConfig, getGlobalLogConfig())
		return
	}

	runLoggerScopeChild(t, "TestSetupMOLoggerReporterCanReenter", childEnv, childArg)
}

func TestQuietTestDebugEncoderPreservesNonDebugBytes(t *testing.T) {
	base := getLoggerEncoder("json")
	for _, level := range []zapcore.Level{zapcore.InfoLevel, zapcore.WarnLevel, zapcore.ErrorLevel} {
		entry := zapcore.Entry{Level: level, Message: "retained", Time: time.Unix(1700000000, 123)}
		fields := []zapcore.Field{zap.String("field", "value"), zap.Int("count", 7)}
		want, err := base.Clone().EncodeEntry(entry, fields)
		require.NoError(t, err)
		quiet := quietTestDebugEncoder{Encoder: base.Clone()}
		got, err := quiet.Clone().EncodeEntry(entry, fields)
		require.NoError(t, err)
		require.Equal(t, string(want.Bytes()), string(got.Bytes()), "level %s must remain unchanged", level)
		want.Free()
		got.Free()
	}
}

type quietDebugObject struct{ calls *atomic.Int32 }

func (o quietDebugObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	o.calls.Add(1)
	enc.AddString("value", "encoded")
	return nil
}

func TestSetupMOLoggerForTestWithoutDebugConsole(t *testing.T) {
	const childEnv = "MO_LOGUTIL_QUIET_TEST_CHILD"
	const childArg = "mo-logutil-quiet-helper"
	if os.Getenv(childEnv) == "1" && len(flag.Args()) == 2 && flag.Arg(0) == childArg {
		original := getTestLoggerState()
		previousReporter := GetReportZapFunc()
		t.Cleanup(func() {
			SetLogReporter(&TraceReporter{ReportZap: previousReporter})
			restoreTestLoggerState(original)
		})
		previousLevel := flag.Arg(1)
		SetupMOLogger(&LogConfig{Level: previousLevel, Format: "json", DisableStore: true})
		previous := getTestLoggerState()
		var traced, encoded atomic.Int32
		SetLogReporter(&TraceReporter{ReportZap: func(enc zapcore.Encoder, ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
			if ent.Level == zapcore.DebugLevel {
				traced.Add(1)
			}
			return enc.EncodeEntry(ent, fields)
		}})
		restore := SetupMOLoggerForTestWithoutDebugConsole(&LogConfig{Level: "debug", Format: "json"})
		t.Cleanup(func() { restore() })
		logger := GetGlobalLogger().With(zap.String("scope", "quiet-probe"))
		require.True(t, logger.Core().Enabled(zapcore.DebugLevel))
		logger.Debug("quiet-debug-marker", zap.Object("probe", quietDebugObject{calls: &encoded}))
		logger.Info("quiet-info-marker")
		logger.Warn("quiet-warn-marker")
		logger.Error("quiet-error-marker")
		require.EqualValues(t, 1, traced.Load(), "trace reporter must receive the DEBUG event")
		require.EqualValues(t, 2, encoded.Load(), "console and trace encoders must execute lazy fields")
		require.True(t, restore())
		requireTestLoggerState(t, previous)
		return
	}

	for _, mode := range []string{"info", "debug"} {
		t.Run(mode, func(t *testing.T) {
			output := runLoggerScopeChild(t, "TestSetupMOLoggerForTestWithoutDebugConsole", childEnv, childArg, mode)
			for _, marker := range []string{"quiet-info-marker", "quiet-warn-marker", "quiet-error-marker"} {
				require.Contains(t, output, marker)
			}
			if mode == "info" {
				require.NotContains(t, output, "quiet-debug-marker")
			} else {
				require.Contains(t, output, "quiet-debug-marker", "a caller's existing DEBUG output must survive")
			}
		})
	}
}

func TestQuietLoggerScopeSetupPanicRestoresState(t *testing.T) {
	original := getTestLoggerState()
	t.Cleanup(func() { restoreTestLoggerState(original) })
	SetupMOLogger(&LogConfig{Level: "info", DisableStore: true})
	previous := getTestLoggerState()
	for _, conf := range []*LogConfig{
		{Level: "not-a-level"},
		{Level: "debug", Format: "unsupported-format"},
	} {
		require.Panics(t, func() { SetupMOLoggerForTestWithoutDebugConsole(conf) })
		requireTestLoggerState(t, previous)
		if !loggerTestScopeMu.TryLock() {
			t.Fatal("test scope mutex remained locked after setup panic")
		}
		loggerTestScopeMu.Unlock()
		restore := SetupMOLoggerForTestWithoutDebugConsole(&LogConfig{Level: "debug"})
		require.True(t, restore())
		requireTestLoggerState(t, previous)
	}
}

func TestTestLoggerScopeAnnouncementFailure(t *testing.T) {
	runLoggerScopeAnnouncementFailure(t, SetupMOLoggerForTest)
}

func TestQuietLoggerScopeAnnouncementFailure(t *testing.T) {
	runLoggerScopeAnnouncementFailure(t, SetupMOLoggerForTestWithoutDebugConsole)
}

func runLoggerScopeAnnouncementFailure(t *testing.T, setup func(*LogConfig) func() bool) {
	t.Helper()
	original := getTestLoggerState()
	previousReporter := GetReportZapFunc()
	t.Cleanup(func() {
		SetLogReporter(&TraceReporter{ReportZap: previousReporter})
		restoreTestLoggerState(original)
	})

	for _, mode := range []string{"panic", "goexit", "setup-panic"} {
		t.Run(mode, func(t *testing.T) {
			SetLogReporter(&TraceReporter{ReportZap: func(zapcore.Encoder, zapcore.Entry, []zapcore.Field) (*buffer.Buffer, error) {
				switch mode {
				case "goexit":
					runtime.Goexit()
				case "setup-panic":
					SetupMOLogger(&LogConfig{Level: "not-a-level"})
				}
				panic("reporter failed")
			}})
			if mode == "panic" {
				require.PanicsWithValue(t, "reporter failed", func() {
					setup(&LogConfig{Level: "debug"})
				})
			} else if mode == "setup-panic" {
				require.Panics(t, func() { setup(&LogConfig{Level: "debug"}) })
			} else {
				done := make(chan struct{})
				go func() {
					defer close(done)
					setup(&LogConfig{Level: "debug"})
				}()
				select {
				case <-done:
				case <-time.After(20 * time.Second):
					t.Fatal("logger setup did not unwind after reporter Goexit")
				}
			}
			requireTestLoggerState(t, original)
			if !loggerTestScopeMu.TryLock() {
				t.Fatal("test scope mutex remained locked")
			}
			loggerTestScopeMu.Unlock()
		})
	}
}

func TestTestLoggerScopeAnnouncementReconfiguration(t *testing.T) {
	runLoggerScopeAnnouncementReconfiguration(t, SetupMOLoggerForTest)
}

func TestQuietLoggerScopeAnnouncementReconfiguration(t *testing.T) {
	runLoggerScopeAnnouncementReconfiguration(t, SetupMOLoggerForTestWithoutDebugConsole)
}

func runLoggerScopeAnnouncementReconfiguration(t *testing.T, setup func(*LogConfig) func() bool) {
	t.Helper()
	original := getTestLoggerState()
	previousReporter := GetReportZapFunc()
	t.Cleanup(func() {
		SetLogReporter(&TraceReporter{ReportZap: previousReporter})
		restoreTestLoggerState(original)
	})
	newConfig := &LogConfig{Level: "error", DisableStore: true}
	var entered atomic.Bool
	var newState testLoggerState
	SetLogReporter(&TraceReporter{ReportZap: func(zapcore.Encoder, zapcore.Entry, []zapcore.Field) (*buffer.Buffer, error) {
		if entered.CompareAndSwap(false, true) {
			SetupMOLogger(newConfig)
			newState = getTestLoggerState()
		}
		return buffer.NewPool().Get(), nil
	}})
	require.PanicsWithValue(t, "logutil: logger reconfigured during test scope setup", func() {
		setup(&LogConfig{Level: "debug"})
		t.Fatal("the test body must not continue after reporter reconfiguration")
	})
	require.True(t, entered.Load())
	require.Same(t, newConfig, getGlobalLogConfig(), "the reporter's newer configuration must survive")
	requireTestLoggerState(t, newState)
	if !loggerTestScopeMu.TryLock() {
		t.Fatal("test scope mutex remained locked after conflict")
	}
	loggerTestScopeMu.Unlock()
}

func TestTestLoggerScopeConcurrentRestore(t *testing.T) {
	runLoggerScopeConcurrentRestore(t, SetupMOLoggerForTest)
}

func TestQuietLoggerScopeConcurrentRestore(t *testing.T) {
	runLoggerScopeConcurrentRestore(t, SetupMOLoggerForTestWithoutDebugConsole)
}

func runLoggerScopeConcurrentRestore(t *testing.T, setup func(*LogConfig) func() bool) {
	t.Helper()
	original := getTestLoggerState()
	t.Cleanup(func() { restoreTestLoggerState(original) })
	restore := setup(&LogConfig{Level: "debug", DisableStore: true})
	t.Cleanup(func() { restore() })
	const callers = 8
	results := make(chan bool, callers)
	for i := 0; i < callers; i++ {
		go func() { results <- restore() }()
	}
	for i := 0; i < callers; i++ {
		select {
		case ok := <-results:
			require.True(t, ok)
		case <-time.After(20 * time.Second):
			t.Fatal("concurrent restore did not finish")
		}
	}
	requireTestLoggerState(t, original)
}

type testEnabledHookCore struct {
	zapcore.Core
	onEnabled func()
}

func (c testEnabledHookCore) Enabled(level zapcore.Level) bool {
	if level == zapcore.DebugLevel {
		c.onEnabled()
	}
	return c.Core.Enabled(level)
}

func TestQuietLoggerScopeEnabledCallbackReconfiguration(t *testing.T) {
	original := getTestLoggerState()
	t.Cleanup(func() { restoreTestLoggerState(original) })
	for _, target := range []string{"global", "skip", "error"} {
		for _, mode := range []string{"reconfigure", "reconfigure-panic", "reconfigure-goexit", "failed-reconfigure", "panic", "goexit"} {
			t.Run(target+"/"+mode, func(t *testing.T) {
				SetupMOLogger(&LogConfig{Level: "info", DisableStore: true})
				previous := getTestLoggerState()
				var expected testLoggerState
				called := false
				hook := testEnabledHookCore{Core: zapcore.NewNopCore(), onEnabled: func() {
					called = true
					switch mode {
					case "reconfigure", "reconfigure-panic", "reconfigure-goexit":
						SetupMOLogger(&LogConfig{Level: "error", DisableStore: true})
						expected = getTestLoggerState()
						if mode == "reconfigure-panic" {
							panic("enabled callback failed")
						}
						if mode == "reconfigure-goexit" {
							runtime.Goexit()
						}
					case "failed-reconfigure":
						invalid := &LogConfig{Level: "not-a-level"}
						expected = getTestLoggerState()
						expected.config = invalid
						SetupMOLogger(invalid)
					case "panic":
						panic("enabled callback failed")
					case "goexit":
						runtime.Goexit()
					}
				}}
				injected := zap.New(hook)
				switch target {
				case "global":
					previous.logger = injected
				case "skip":
					previous.skip = injected
				case "error":
					previous.err = injected
				}
				restoreTestLoggerState(previous)
				if mode == "panic" || mode == "goexit" {
					expected = previous
				}
				if mode == "goexit" || mode == "reconfigure-goexit" {
					done := make(chan struct{})
					go func() {
						defer close(done)
						SetupMOLoggerForTestWithoutDebugConsole(&LogConfig{Level: "debug"})
					}()
					select {
					case <-done:
					case <-time.After(20 * time.Second):
						t.Fatal("enabled callback Goexit did not release scope")
					}
				} else {
					require.Panics(t, func() {
						SetupMOLoggerForTestWithoutDebugConsole(&LogConfig{Level: "debug"})
					})
				}
				require.True(t, called, "selected core must be queried")
				requireTestLoggerState(t, expected)
				if !loggerTestScopeMu.TryLock() {
					t.Fatal("enabled callback left the scope mutex locked")
				}
				loggerTestScopeMu.Unlock()
			})
		}
	}
}
