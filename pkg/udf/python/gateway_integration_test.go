// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package python

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/udf/protocol"
	"github.com/stretchr/testify/require"
)

var realWorkerBenchmarkRun atomic.Uint64

func TestGatewayExecutesAgainstRealPythonWorker(t *testing.T) {
	python, err := exec.LookPath("python3")
	require.NoError(t, err, "python3 is required for the real worker contract test")
	workerPath := realWorkerPath(t)
	port := freeTCPPort(t)

	cmd := startRealWorker(t, python, workerPath, port)
	artifactFS, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	artifactStore, err := NewFileArtifactStore(artifactFS, DefaultMaxArtifactBytes)
	require.NoError(t, err)

	cfg := ClientConfig{
		Enabled:         true,
		AllowUnisolated: true,
		ServerAddress:   "127.0.0.1:" + strconv.Itoa(port),
		MaxBatchBytes:   1 << 20,
		// Keep the integration path deliberately smaller than the input so a
		// single Gateway.Execute exercises more than one W=1 batch and the
		// bounded handler burst reuse contract.
		MaxBatchRows:         2,
		MaxActiveInvocations: 2,
		RequestTimeout:       5 * time.Second,
		MaxTerminalEntries:   32,
		MaxTerminalBytes:     1 << 20,
		TerminalRecordTTL:    time.Minute,
	}
	gateway, err := NewGatewayWithArtifactStore(cfg, artifactStore)
	require.NoError(t, err)
	defer func() {
		_ = gateway.Close()
		stopRealWorker(cmd)
	}()

	readyContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	require.NoError(t, gateway.CheckLanguageReady(readyContext, udf.LanguagePython))
	cancel()

	t.Run("scalar", func(t *testing.T) {
		input, mp := integrationInput(t, []int64{1, 2, 3})
		defer func() {
			input.Free(mp)
			mpool.DeleteMPool(mp)
		}()
		result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
		defer result.Free()
		invocation := integrationInvocation(ModeScalar, "def add(ctx, value): return value + 10", input, protocol.FencingTuple{
			AccountID: 1, StatementID: "real-flight-scalar", GroupID: "real-flight-scalar-group",
			GroupEpoch: 1, InvocationID: "real-flight-scalar-invocation", LeaseEpoch: 1,
		})
		publishIntegrationArtifact(t, artifactStore, invocation)
		require.NoError(t, gateway.Execute(context.Background(), invocation, result, mp))
		require.Equal(t, uint64(1), invocation.Tuple.LeaseEpoch, "Gateway must not mutate the caller's tuple when attaching the worker lease")
		require.Equal(t, []int64{11, 12, 13}, vector.MustFixedColNoTypeCheck[int64](result.GetResultVector()))
	})

	t.Run("zero-argument scalar", func(t *testing.T) {
		mp := mpool.MustNewZeroNoFixed()
		defer mpool.DeleteMPool(mp)
		result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
		defer result.Free()
		invocation := integrationInvocationForArgs(
			ModeScalar,
			"add",
			"def add(ctx): return 42",
			nil,
			nil,
			1,
			protocol.FencingTuple{
				AccountID: 1, StatementID: "real-flight-zero-arg-scalar", GroupID: "real-flight-zero-arg-scalar-group",
				GroupEpoch: 1, InvocationID: "real-flight-zero-arg-scalar-invocation", LeaseEpoch: 1,
			},
		)
		publishIntegrationArtifact(t, artifactStore, invocation)
		require.NoError(t, gateway.Execute(context.Background(), invocation, result, mp))
		require.Equal(t, []int64{42}, vector.MustFixedColNoTypeCheck[int64](result.GetResultVector()))
	})

	t.Run("vector", func(t *testing.T) {
		input, mp := integrationInput(t, []int64{4, 5, 6})
		defer func() {
			input.Free(mp)
			mpool.DeleteMPool(mp)
		}()
		result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
		defer result.Free()
		invocation := integrationInvocation(
			ModeVector,
			"import pyarrow as pa\ndef add(ctx, values): return pa.array([value.as_py() + 20 for value in values], type=pa.int64())",
			input,
			protocol.FencingTuple{
				AccountID: 1, StatementID: "real-flight-vector", GroupID: "real-flight-vector-group",
				GroupEpoch: 1, InvocationID: "real-flight-vector-invocation", LeaseEpoch: 1,
			},
		)
		publishIntegrationArtifact(t, artifactStore, invocation)
		require.NoError(t, gateway.Execute(context.Background(), invocation, result, mp))
		require.Equal(t, []int64{24, 25, 26}, vector.MustFixedColNoTypeCheck[int64](result.GetResultVector()))
	})

	t.Run("zero-argument vector", func(t *testing.T) {
		const rows = 4
		resultMP := mpool.MustNewZeroNoFixed()
		defer mpool.DeleteMPool(resultMP)
		result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), resultMP)
		defer result.Free()
		source := "import pyarrow as pa\ndef add(ctx): return pa.array([ctx.num_rows] * ctx.num_rows, type=pa.int64())"
		invocation := integrationZeroArgVectorInvocation(source, rows, protocol.FencingTuple{
			AccountID: 1, StatementID: "real-flight-zero-arg", GroupID: "real-flight-zero-arg-group",
			GroupEpoch: 1, InvocationID: "real-flight-zero-arg-invocation", LeaseEpoch: 1,
		})
		publishIntegrationArtifact(t, artifactStore, invocation)
		require.NoError(t, gateway.Execute(context.Background(), invocation, result, resultMP))
		// MaxBatchRows is two, so each vector invocation receives the exact
		// physical batch shape rather than the total statement length.
		require.Equal(t, []int64{2, 2, 2, 2}, vector.MustFixedColNoTypeCheck[int64](result.GetResultVector()))
	})
}

func TestSupervisorExecutesAgainstRealPythonWorker(t *testing.T) {
	python, err := exec.LookPath("python3")
	require.NoError(t, err, "python3 is required for the Supervisor integration test")
	workerPath := realWorkerPath(t)
	port := freeTCPPort(t)
	supervisor, err := NewSupervisor(Config{
		Address: "127.0.0.1:" + strconv.Itoa(port),
		Path:    filepath.Dir(workerPath),
		Python:  python,
	})
	require.NoError(t, err)
	require.NoError(t, supervisor.Start())
	workerDone := supervisor.Done()
	require.NotNil(t, workerDone)
	t.Cleanup(func() {
		_ = supervisor.Close()
	})
	waitForWorkerPort(t, port)

	artifactFS, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	artifactStore, err := NewFileArtifactStore(artifactFS, DefaultMaxArtifactBytes)
	require.NoError(t, err)
	gateway, err := NewGatewayWithArtifactStore(ClientConfig{
		Enabled:                  true,
		AllowUnisolated:          true,
		ServerAddress:            "127.0.0.1:" + strconv.Itoa(port),
		MaxBatchBytes:            1 << 20,
		MaxBatchRows:             2,
		MaxInvocationRows:        32,
		MaxInvocationResultBytes: 1 << 20,
		MaxActiveInvocations:     1,
		RequestTimeout:           5 * time.Second,
		MaxTerminalEntries:       16,
		MaxTerminalBytes:         1 << 20,
		TerminalRecordTTL:        time.Minute,
	}, artifactStore)
	require.NoError(t, err)
	defer gateway.Close()
	readyContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	require.NoError(t, gateway.CheckLanguageReady(readyContext, udf.LanguagePython))
	cancel()

	input, mp := integrationInput(t, []int64{5, 6, 7})
	defer func() {
		input.Free(mp)
		mpool.DeleteMPool(mp)
	}()
	result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
	defer result.Free()
	invocation := integrationInvocation(ModeScalar, "def add(ctx, value): return value + 30", input, protocol.FencingTuple{
		AccountID: 1, StatementID: "supervisor-flight", GroupID: "supervisor-flight-group",
		GroupEpoch: 1, InvocationID: "supervisor-flight-invocation", LeaseEpoch: 1,
	})
	publishIntegrationArtifact(t, artifactStore, invocation)
	require.NoError(t, gateway.Execute(context.Background(), invocation, result, mp))
	require.Equal(t, []int64{35, 36, 37}, vector.MustFixedColNoTypeCheck[int64](result.GetResultVector()))
	select {
	case <-workerDone:
		t.Fatal("Python worker exited during a successful Supervisor integration")
	default:
	}
}

func publishIntegrationArtifact(t testing.TB, store *FileArtifactStore, invocation *udf.Invocation) {
	t.Helper()
	source := invocation.Source
	require.NotEmpty(t, source)
	_, err := store.Publish(context.Background(), invocation.FunctionRef.AccountID, invocation.Handler, source)
	require.NoError(t, err)
	invocation.Source = ""
}

func TestGatewayRejectsThePreviousWorkerLeaseAfterRestart(t *testing.T) {
	python, err := exec.LookPath("python3")
	require.NoError(t, err, "python3 is required for the real worker contract test")
	workerPath := realWorkerPath(t)
	port := freeTCPPort(t)
	first := startRealWorker(t, python, workerPath, port)

	cfg := ClientConfig{
		Enabled:              true,
		AllowUnisolated:      true,
		ServerAddress:        "127.0.0.1:" + strconv.Itoa(port),
		MaxBatchBytes:        1 << 20,
		MaxBatchRows:         1024,
		MaxActiveInvocations: 1,
		RequestTimeout:       2 * time.Second,
		MaxTerminalEntries:   32,
		MaxTerminalBytes:     1 << 20,
		TerminalRecordTTL:    time.Minute,
	}
	gateway, err := NewGateway(cfg)
	require.NoError(t, err)
	defer gateway.Close()
	readyContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	require.NoError(t, gateway.CheckLanguageReady(readyContext, udf.LanguagePython))
	cancel()

	stopRealWorker(first)
	second := startRealWorker(t, python, workerPath, port)
	defer stopRealWorker(second)

	input, mp := integrationInput(t, []int64{9})
	defer func() {
		input.Free(mp)
		mpool.DeleteMPool(mp)
	}()
	firstResult := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
	defer firstResult.Free()
	firstInvocation := integrationInvocation(ModeScalar, "def add(ctx, value): return value + 1", input, protocol.FencingTuple{
		AccountID: 1, StatementID: "restart-statement", GroupID: "restart-group", GroupEpoch: 1,
		InvocationID: "restart-invocation", LeaseEpoch: 1,
	})
	// The Gateway has a cached lease from the first worker. The second worker
	// must reject it before handler admission; the failed invocation is never
	// transparently replayed.
	require.Error(t, gateway.Execute(context.Background(), firstInvocation, firstResult, mp))
	require.Equal(t, uint64(1), firstInvocation.Tuple.LeaseEpoch)

	secondResult := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
	defer secondResult.Free()
	secondInvocation := integrationInvocation(ModeScalar, "def add(ctx, value): return value + 1", input, protocol.FencingTuple{
		AccountID: 1, StatementID: "restart-statement", GroupID: "restart-group-2", GroupEpoch: 1,
		InvocationID: "restart-invocation-2", LeaseEpoch: 1,
	})
	require.NoError(t, gateway.Execute(context.Background(), secondInvocation, secondResult, mp))
	require.Equal(t, []int64{10}, vector.MustFixedColNoTypeCheck[int64](secondResult.GetResultVector()))
}

func TestGatewayCloseCancelsAnInFlightExchange(t *testing.T) {
	python, err := exec.LookPath("python3")
	require.NoError(t, err, "python3 is required for the real worker contract test")
	workerPath := realWorkerPath(t)
	port := freeTCPPort(t)
	worker := startRealWorker(t, python, workerPath, port)

	marker := filepath.Join(t.TempDir(), "handler-started")
	// The handler has a deterministic start barrier and then never returns on
	// its own. Gateway.Close must cancel the Flight exchange and cause the
	// worker's independent cleanup path to terminate this process group.
	source := fmt.Sprintf(`import pathlib
import time

def add(ctx, value):
    pathlib.Path(%q).write_text(str(__import__("os").getpid()))
    while True:
        time.sleep(1)
`, marker)

	gateway, err := NewGateway(ClientConfig{
		Enabled:              true,
		AllowUnisolated:      true,
		ServerAddress:        "127.0.0.1:" + strconv.Itoa(port),
		MaxBatchBytes:        1 << 20,
		MaxBatchRows:         1,
		MaxActiveInvocations: 1,
		RequestTimeout:       30 * time.Second,
		MaxTerminalEntries:   32,
		MaxTerminalBytes:     1 << 20,
		TerminalRecordTTL:    time.Minute,
	})
	require.NoError(t, err)
	defer func() {
		_ = gateway.Close()
		stopRealWorker(worker)
	}()

	readyContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	require.NoError(t, gateway.CheckLanguageReady(readyContext, udf.LanguagePython))
	cancel()

	input, mp := integrationInput(t, []int64{1})
	result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
	invocation := integrationInvocation(ModeScalar, source, input, protocol.FencingTuple{
		AccountID: 1, StatementID: "gateway-close", GroupID: "gateway-close-group",
		GroupEpoch: 1, InvocationID: "gateway-close-invocation", LeaseEpoch: 1,
	})
	done := make(chan error, 1)
	go func() {
		done <- gateway.Execute(context.Background(), invocation, result, mp)
	}()

	require.Eventually(t, func() bool {
		_, statErr := os.Stat(marker)
		return statErr == nil
	}, 5*time.Second, 10*time.Millisecond, "handler did not reach the cancellation barrier")
	require.NoError(t, gateway.Close())
	select {
	case err := <-done:
		require.Error(t, err, "closing the Gateway must not report a successful in-flight invocation")
	case <-time.After(5 * time.Second):
		t.Fatal("Gateway.Close did not cancel the in-flight exchange")
	}
	pidBytes, err := os.ReadFile(marker)
	require.NoError(t, err)
	handlerPID, err := strconv.Atoi(string(pidBytes))
	require.NoError(t, err)
	if runtime.GOOS != "windows" {
		require.Eventually(t, func() bool {
			return syscall.Kill(handlerPID, 0) != nil
		}, 5*time.Second, 10*time.Millisecond, "handler process survived Gateway.Close")
	}
	// The transport error is only half of the shutdown contract.  The owner
	// cleanup must also have removed the active admission, released K, and
	// retained exactly one terminal fence for the accepted Open.  Inspect these
	// after Execute has returned so the assertion is about the real Flight path,
	// rather than a helper's cleanup callback.
	gateway.admissionMu.Lock()
	require.Empty(t, gateway.admittedGroups)
	require.Empty(t, gateway.active)
	closedEpoch := gateway.closedGroups[invocation.Tuple.GroupID]
	gateway.admissionMu.Unlock()
	require.Equal(t, uint64(1), closedEpoch)
	entries, _ := gateway.ledger.Counts()
	require.Equal(t, 1, entries, "accepted Open retains one terminal tombstone")

	input.Free(mp)
	result.Free()
	mpool.DeleteMPool(mp)
}

// BenchmarkGatewayRealPythonWorkerBurst measures the real Flight path with a
// fixed two-row batch. The first sub-benchmark performs eight independent
// one-batch invocations; the second performs one invocation containing eight
// batches. The handler records module loads in a test-owned file, which makes
// the child/import count observable instead of inferring reuse from elapsed
// time. This is a W=1 comparison: it measures bounded burst reuse and does not
// claim a W>1 pipeline benefit.
func BenchmarkGatewayRealPythonWorkerBurst(b *testing.B) {
	python, err := exec.LookPath("python3")
	if err != nil {
		b.Skip("python3 is required for the real worker benchmark")
	}
	workerPath := realWorkerPath(b)
	port := freeTCPPort(b)
	worker := startRealWorker(b, python, workerPath, port)
	defer stopRealWorker(worker)

	gateway, err := NewGateway(ClientConfig{
		Enabled:                  true,
		AllowUnisolated:          true,
		ServerAddress:            "127.0.0.1:" + strconv.Itoa(port),
		MaxBatchBytes:            1 << 20,
		MaxBatchRows:             2,
		MaxActiveInvocations:     2,
		MaxInvocationRows:        1024,
		MaxInvocationResultBytes: 1 << 20,
		RequestTimeout:           30 * time.Second,
		MaxTerminalEntries:       1024,
		MaxTerminalBytes:         1 << 20,
		TerminalRecordTTL:        time.Minute,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer gateway.Close()
	readyContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := gateway.CheckLanguageReady(readyContext, udf.LanguagePython); err != nil {
		cancel()
		b.Fatal(err)
	}
	cancel()

	benchmark := func(b *testing.B, rows, calls int, label string) {
		marker := filepath.Join(b.TempDir(), label+"-module-loads")
		runID := realWorkerBenchmarkRun.Add(1)
		source := fmt.Sprintf(`from pathlib import Path
with Path(%q).open("a", encoding="utf-8") as marker:
    marker.write("loaded\n")

def add(ctx, value):
    return value + 1
`, marker)
		input, mp := integrationInput(b, make([]int64, rows))
		for index := range make([]int64, rows) {
			vector.MustFixedColNoTypeCheck[int64](input)[index] = int64(index)
		}
		result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
		defer func() {
			input.Free(mp)
			result.Free()
			mpool.DeleteMPool(mp)
		}()

		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			for call := 0; call < calls; call++ {
				invocation := integrationInvocation(
					ModeScalar, source, input,
					protocol.FencingTuple{
						AccountID:    1,
						StatementID:  fmt.Sprintf("benchmark-%s-%d-%d-%d", label, runID, iteration, call),
						GroupID:      fmt.Sprintf("benchmark-%s-group-%d-%d-%d", label, runID, iteration, call),
						GroupEpoch:   1,
						InvocationID: fmt.Sprintf("benchmark-%s-invocation-%d-%d-%d", label, runID, iteration, call),
						LeaseEpoch:   1,
					},
				)
				if err := gateway.Execute(context.Background(), invocation, result, mp); err != nil {
					b.Fatal(err)
				}
			}
		}
		b.StopTimer()
		loads, err := os.ReadFile(marker)
		if err != nil {
			b.Fatal(err)
		}
		actualLoads := strings.Count(string(loads), "loaded\n")
		if actualLoads == 0 {
			b.Fatal("the handler did not record a module load")
		}
		b.ReportMetric(float64(rows*calls*b.N)/float64(b.Elapsed().Nanoseconds())*1e9, "rows/s")
		b.Logf("batches/invocation=%d module_loads=%d", rows/2, actualLoads)
	}

	b.Run("eight_independent_one_batch_invocations", func(b *testing.B) {
		benchmark(b, 2, 8, "independent")
	})
	b.Run("one_eight_batch_invocation", func(b *testing.B) {
		benchmark(b, 16, 1, "burst")
	})
}

func realWorkerPath(t testing.TB) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	path := filepath.Join(filepath.Dir(sourceFile), "worker", "worker.py")
	_, err := os.Stat(path)
	require.NoError(t, err)
	return path
}

func freeTCPPort(t testing.TB) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitForWorkerPort(t testing.TB, port int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		connection, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), 100*time.Millisecond)
		if err == nil {
			_ = connection.Close()
			return
		}
		if time.Now().After(deadline) {
			require.NoError(t, err, "Python worker did not listen before the startup deadline")
		}
		<-ticker.C
	}
}

func startRealWorker(t testing.TB, python, workerPath string, port int) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(python, workerPath, "--address=grpc+tcp://127.0.0.1:"+strconv.Itoa(port))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), "PYTHONUNBUFFERED=1")
	if runtime.GOOS != "windows" {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	require.NoError(t, cmd.Start())
	waitForWorkerPort(t, port)
	return cmd
}

func stopRealWorker(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil {
		return
	}
	if runtime.GOOS != "windows" {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	} else {
		_ = cmd.Process.Kill()
	}
	_, _ = cmd.Process.Wait()
}

func integrationInput(t testing.TB, values []int64) (*vector.Vector, *mpool.MPool) {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	input := vector.NewVec(types.T_int64.ToType())
	for _, value := range values {
		require.NoError(t, vector.AppendFixed(input, value, false, mp))
	}
	return input, mp
}

func integrationInvocation(mode, source string, input *vector.Vector, tuple protocol.FencingTuple) *udf.Invocation {
	return integrationInvocationForArgs(
		mode, "add", source,
		[]types.Type{types.T_int64.ToType()}, []*vector.Vector{input}, input.Length(), tuple,
	)
}

func integrationZeroArgVectorInvocation(source string, length int, tuple protocol.FencingTuple) *udf.Invocation {
	return integrationInvocationForArgs(ModeVector, "add", source, nil, nil, length, tuple)
}

func integrationInvocationForArgs(
	mode, handler, source string,
	args []types.Type,
	inputs []*vector.Vector,
	length int,
	tuple protocol.FencingTuple,
) *udf.Invocation {
	environmentDigest := func() string {
		digest, _ := udf.PythonEnvironmentDigest()
		return digest
	}()
	argumentDescriptors := make([]TypeDescriptor, len(args))
	for index, typ := range args {
		argumentDescriptors[index], _ = NewTypeDescriptor(typ)
	}
	returnDescriptor, _ := NewTypeDescriptor(types.T_int64.ToType())
	statementValues := map[string]string{
		"statement_id":                    tuple.StatementID,
		"statement_timestamp_utc":         "1704067200123456",
		"session_timezone_kind":           "FIXED_OFFSET",
		"session_timezone_offset_minutes": "+480",
		"sql_mode":                        "[\"ANSI\",\"STRICT_TRANS_TABLES\"]",
		"current_database":                "udf_integration",
		"current_user":                    "root",
		"current_role":                    "writer",
		"connection_collation":            "utf8mb4_bin",
	}
	statementContext, _ := udf.StatementContextFromMap(statementValues)
	return &udf.Invocation{
		FunctionRef: udf.FunctionRef{
			AccountID: tuple.AccountID, DatabaseID: 2, FunctionID: 3, Revision: 1, NamespaceVersion: 1,
		},
		Language:                udf.LanguagePython,
		Handler:                 handler,
		Source:                  source,
		Args:                    args,
		ReturnType:              types.T_int64.ToType(),
		Inputs:                  inputs,
		Length:                  length,
		Mode:                    mode,
		NullPolicy:              NullCallHandler,
		ABIContract:             udf.PythonABIContract,
		AdapterVersion:          udf.PythonAdapterVersion,
		SDKVersion:              udf.PythonSDKVersion,
		DefinitionSchemaVersion: udf.PythonDefinitionSchemaVersion,
		ArtifactDigest:          udf.PythonInlineArtifactDigest(handler, source),
		EnvironmentDigest:       environmentDigest,
		DefinitionFingerprint: func() string {
			fingerprint, _ := DefinitionFingerprint(
				udf.PythonDefinitionSchemaVersion,
				handler, mode, NullCallHandler,
				udf.PythonABIContract, udf.PythonAdapterVersion,
				udf.PythonInlineArtifactDigest(handler, source), environmentDigest,
				udf.PythonSDKVersion, argumentDescriptors, returnDescriptor,
			)
			return fingerprint
		}(),
		CallsiteID:       "python/integration",
		MayError:         true,
		SecurityMode:     "INVOKER",
		StatementContext: statementContext,
		SecurityFrame: &udf.SecurityFrame{
			ContractVersion: udf.SecurityFrameContractVersion,
			Mode:            "INVOKER",
		},
		Context: statementValues,
		Tuple:   tuple,
	}
}
