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

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	args := []string{"-test.run=^TestSetupMOLoggerReporterCanReenter$", "-test.timeout=15s"}
	if f := flag.Lookup("test.gocoverdir"); f != nil && f.Value.String() != "" {
		args = append(args, "-test.gocoverdir="+f.Value.String())
	}
	args = append(args, "--", childArg)
	cmd := exec.CommandContext(ctx, os.Args[0], args...)
	cmd.WaitDelay = 3 * time.Second
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, childEnv+"=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, childEnv+"=1")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "reporter reentry child failed: %s", output)
}

func TestTestLoggerScopeAnnouncementFailure(t *testing.T) {
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
					SetupMOLoggerForTest(&LogConfig{Level: "debug"})
				})
			} else if mode == "setup-panic" {
				require.Panics(t, func() { SetupMOLoggerForTest(&LogConfig{Level: "debug"}) })
			} else {
				done := make(chan struct{})
				go func() {
					defer close(done)
					SetupMOLoggerForTest(&LogConfig{Level: "debug"})
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
		SetupMOLoggerForTest(&LogConfig{Level: "debug"})
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
	original := getTestLoggerState()
	t.Cleanup(func() { restoreTestLoggerState(original) })
	restore := SetupMOLoggerForTest(&LogConfig{Level: "debug", DisableStore: true})
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
