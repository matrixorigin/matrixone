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

package upgrade

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pythonudf "github.com/matrixorigin/matrixone/pkg/udf/python"
	"github.com/stretchr/testify/require"
)

// This fixture adds one opt-in worker to the existing embedded SQL topology.
// An ordinary shared cluster cannot validate Python CREATE or Flight results.
// One row and one connection cover publication, replacement and nested binding.
func TestPythonRoutineSQLBoundaries(t *testing.T) {
	python, err := exec.LookPath("python3")
	require.NoError(t, err)
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	worker := filepath.Clean(filepath.Join(filepath.Dir(file), "../../udf/python/worker/worker.py"))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	directory := t.TempDir()
	log, err := os.Create(filepath.Join(directory, "worker.log"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, log.Close()) })
	cmd := exec.Command(python, "-u", "-X", "faulthandler", worker, "--address=grpc+tcp://"+address)
	cmd.Dir = directory
	cmd.Stdout = log
	cmd.Stderr = log
	started := time.Now()
	require.NoError(t, cmd.Start())
	t.Logf("owned worker pid=%d start=%s cwd=%s config=%s data=%s", cmd.Process.Pid, started.Format(time.RFC3339Nano), directory, strings.Join(cmd.Args, " "), directory)
	type workerExit struct {
		err error
		at  time.Time
	}
	workerDone := make(chan workerExit, 1)
	go func() { workerDone <- workerExit{err: cmd.Wait(), at: time.Now()} }()
	t.Cleanup(func() {
		var exit workerExit
		var killErr error
		requestedTermination := false
		select {
		case exit = <-workerDone:
		default:
			requestedTermination = true
			killErr = cmd.Process.Kill()
			exit = <-workerDone
		}
		t.Logf("owned worker pid=%d exit=%s status=%v teardown_requested=%t kill_error=%v", cmd.Process.Pid, exit.at.Format(time.RFC3339Nano), exit.err, requestedTermination, killErr)
		if !requestedTermination {
			t.Errorf("Python worker exited before teardown: %v", exit.err)
		}
		if t.Failed() {
			info, statErr := log.Stat()
			if statErr != nil {
				t.Logf("worker log stat: %v", statErr)
				return
			}
			tail := make([]byte, min(info.Size(), 64<<10))
			n, readErr := log.ReadAt(tail, info.Size()-int64(len(tail)))
			t.Logf("worker log bytes=%d tail_bytes=%d truncated=%t read_error=%v:\n%s", info.Size(), n, info.Size() > int64(n), readErr, tail[:n])
		}
	})
	deadline := time.Now().Add(15 * time.Second)
	for {
		conn, dialErr := net.DialTimeout("tcp", address, time.Second)
		if dialErr == nil {
			require.NoError(t, conn.Close())
			break
		}
		require.True(t, time.Now().Before(deadline), "worker not ready: %v", dialErr)
		time.Sleep(10 * time.Millisecond) // observable port readiness, bounded by deadline
	}
	cluster, err := embed.NewCluster(embed.WithTesting(), embed.WithCNCount(1), embed.WithPreStart(func(svc embed.ServiceOperator) {
		if svc.ServiceType() == metadata.ServiceType_CN {
			svc.Adjust(func(cfg *embed.ServiceConfig) {
				cfg.CN.PythonUdfClient = pythonudf.ClientConfig{Enabled: true, AllowUnisolated: true, ServerAddress: address}
				cfg.CN.Frontend.SkipCheckUser = false
			})
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	require.NoError(t, cluster.Start())
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	t.Logf("owned SQL cluster data=%s", cn.GetServiceConfig().DataDir)
	db := openViewDescriptionDB(t, cn.GetServiceConfig().CN.Frontend.Port, "dump:111")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_, err = db.ExecContext(ctx, "create database python_boundary")
	require.NoError(t, err)
	t.Cleanup(func() { cleanupViewDescriptionSQL(t, db, "drop database python_boundary") })
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	// These SQL boundaries do not depend on the host's SYSTEM timezone identity.
	_, err = conn.ExecContext(ctx, "set session time_zone = '+00:00'")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "use python_boundary")
	require.NoError(t, err)
	create := func(name, result, source, mode string) error {
		_, err := conn.ExecContext(ctx, fmt.Sprintf("create function %s() returns %s language python as '%s' handler 'f' mode %s", name, result, strings.ReplaceAll(source, "'", "''"), mode))
		return err
	}
	t.Run("nested_zero_argument_blob", func(t *testing.T) {
		require.NoError(t, create("valid_blob", "blob", "def f(ctx): return bytes([120])", "scalar"))
		var n int
		require.NoError(t, conn.QueryRowContext(ctx, "select octet_length(valid_blob())").Scan(&n))
		require.Equal(t, 1, n)
	})
	t.Run("bounded_scalar_and_vector", func(t *testing.T) {
		for _, mode := range []string{"scalar", "vector"} {
			for _, family := range []string{"tinytext", "tinyblob"} {
				for _, size := range []int{255, 256} {
					name := fmt.Sprintf("%s_%s_%d", mode, family, size)
					value := fmt.Sprintf("chr(120)*%d", size)
					if family == "tinyblob" {
						value = fmt.Sprintf("bytes([120])*%d", size)
					}
					if mode == "vector" {
						value = "__import__(\"pyarrow\").array([" + value + "])"
					}
					require.NoError(t, create(name, family, "def f(ctx): return "+value, mode))
					var result []byte
					err := conn.QueryRowContext(ctx, "select "+name+"()").Scan(&result)
					if size == 255 {
						require.NoError(t, err)
						require.Len(t, result, 255)
					} else {
						require.ErrorContains(t, err, "TYPE_CONTRACT")
					}
				}
			}
		}
		require.NoError(t, create("utf8_text", "tinytext", "def f(ctx): return chr(20013)*86", "scalar"))
		var out string
		require.ErrorContains(t, conn.QueryRowContext(ctx, "select utf8_text()").Scan(&out), "TYPE_CONTRACT")
		require.NoError(t, create("utf8_varchar", "varchar(255)", "def f(ctx): return chr(20013)*86", "scalar"))
		require.NoError(t, conn.QueryRowContext(ctx, "select utf8_varchar()").Scan(&out))
		require.Equal(t, strings.Repeat("中", 86), out)
	})
	t.Run("failed_publication_recovery_and_shared_digest", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			require.Error(t, create("bad_artifact", "int", fmt.Sprintf("def f( %d", i), "scalar"))
		}
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_user_defined_function where db='python_boundary' and name='bad_artifact'").Scan(&count))
		require.Zero(t, count)
		const source = "def f(ctx): return 43"
		// Validation succeeds, but the missing target database rejects the
		// catalog transaction after publication. Retrying reuses the digest.
		require.Error(t, create("python_boundary_missing.recovered_artifact", "int", source, "scalar"))
		require.NoError(t, create("recovered_artifact", "int", source, "scalar"))
		require.NoError(t, create("shared_artifact_peer", "int", source, "scalar"))
		_, err := conn.ExecContext(ctx, "create or replace function shared_artifact_peer() returns int language python as 'def f(' handler 'f'")
		require.Error(t, err)
		var result int
		require.NoError(t, conn.QueryRowContext(ctx, "select shared_artifact_peer()").Scan(&result))
		require.Equal(t, 43, result)
		_, err = conn.ExecContext(ctx, "drop function shared_artifact_peer()")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, "select recovered_artifact()").Scan(&result))
		require.Equal(t, 43, result, "dropping one reference must not delete another revision's source")
	})
	t.Run("attributed_decimal_declarations_fail_closed", func(t *testing.T) {
		for _, suffix := range []string{" unsigned", " zerofill"} {
			require.ErrorContains(t, create("bad_decimal", "decimal(8,2)"+suffix, "def f(ctx): return 7", "scalar"), "attributed DECIMAL")
			_, err := conn.ExecContext(ctx, "create function bad_decimal(x decimal(8,2)"+suffix+") returns int language python as 'def f(ctx, x): return 7' handler 'f'")
			require.ErrorContains(t, err, "attributed DECIMAL")
		}
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_user_defined_function where db='python_boundary' and name='bad_decimal'").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, create("plain_decimal", "decimal(8,2)", "def f(ctx): return __import__(\"decimal\").Decimal(\"7.00\")", "scalar"))
		var result string
		require.NoError(t, conn.QueryRowContext(ctx, "select plain_decimal()").Scan(&result))
		require.Equal(t, "7.00", result)
	})
	t.Run("catalog_envelope_and_failed_replacement", func(t *testing.T) {
		require.NoError(t, create("keep_revision", "int", "def f(ctx): return 7", "scalar"))
		var saved string
		require.NoError(t, conn.QueryRowContext(ctx, "select body from mo_catalog.mo_user_defined_function where db='python_boundary' and name='keep_revision'").Scan(&saved))
		var envelope map[string]any
		require.NoError(t, json.Unmarshal([]byte(saved), &envelope))
		baseSource := "def f(ctx): return 8\n#"
		envelope["source"] = baseSource
		encoded, encodeErr := json.Marshal(envelope)
		require.NoError(t, encodeErr)
		boundarySource := baseSource + strings.Repeat("x", 65535-len(encoded))
		require.NoError(t, create("exact_body_bound", "int", boundarySource, "scalar"))
		var storedBytes int
		require.NoError(t, conn.QueryRowContext(ctx, "select octet_length(body) from mo_catalog.mo_user_defined_function where db='python_boundary' and name='exact_body_bound'").Scan(&storedBytes))
		require.Equal(t, 65535, storedBytes)
		require.ErrorContains(t, create("over_body_bound", "int", boundarySource+"x", "scalar"), "encoded catalog body exceeds")
		var before uint64
		require.NoError(t, conn.QueryRowContext(ctx, "select active_revision from mo_catalog.mo_user_defined_function where db='python_boundary' and name='keep_revision'").Scan(&before))
		source := "def f(ctx): return 8\n#" + strings.Repeat("x", 65109-len("def f(ctx): return 8\n#"))
		require.Len(t, source, 65109)
		_, err = conn.ExecContext(ctx, fmt.Sprintf("create function too_large(value bigint) returns bigint language python as '%s' handler 'py_echo'", source))
		require.ErrorContains(t, err, "encoded catalog body exceeds")
		statement := fmt.Sprintf("create or replace function keep_revision() returns int language python as '%s' handler 'f'", boundarySource+"x")
		_, err := conn.ExecContext(ctx, statement)
		require.ErrorContains(t, err, "encoded catalog body exceeds")
		var after uint64
		require.NoError(t, conn.QueryRowContext(ctx, "select active_revision from mo_catalog.mo_user_defined_function where db='python_boundary' and name='keep_revision'").Scan(&after))
		require.Equal(t, before, after)
		var result int
		require.NoError(t, conn.QueryRowContext(ctx, "select keep_revision()").Scan(&result))
		require.Equal(t, 7, result)
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_user_defined_function where db='python_boundary' and name='too_large'").Scan(&count))
		require.Zero(t, count)
	})
}
