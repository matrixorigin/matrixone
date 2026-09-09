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

package arrowload

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

// testArrowLoadLocalMinIO extends the 1-CN public-path fixture across the real
// S3-compatible boundary. A dedicated MinIO process is required here because
// an in-memory object-store fake cannot prove credential parsing, HTTP request
// cancellation, conditional ETag reads, or stage expansion.
func testArrowLoadLocalMinIO(t *testing.T, db *sql.DB) {
	server := startArrowLoadMinIO(t)

	originalPath := fixtureIDName(t, t.TempDir(), "original.arrow", containerFile,
		[][]idNameRow{{{id: 1, name: "first"}, {id: 2, name: "second"}}})
	replacementPath := fixtureIDName(t, t.TempDir(), "replacement.arrow", containerFile,
		[][]idNameRow{{{id: 7, name: "new-first"}, {id: 8, name: "new-second"}}})
	streamPath := fixtureIDName(t, t.TempDir(), "source.stream", containerStream,
		[][]idNameRow{{{id: 3, name: "stream-a"}}, {{id: 4, name: "stream-b"}}})
	original := mustReadFile(t, originalPath)
	replacement := mustReadFile(t, replacementPath)
	stream := mustReadFile(t, streamPath)

	t.Run("DirectFileAndObjectRefresh", func(t *testing.T) {
		const key = "direct/source.arrow"
		server.put(t, key, original)
		mustExec(t, db, "drop table if exists minio_direct_file")
		mustExec(t, db, "create table minio_direct_file(id bigint not null, name varchar(50))")
		mustExec(t, db, minioLoadSQL(server.endpointURL, server, key, "minio_direct_file", "file", false))
		require.Equal(t, int64(2), queryCount(t, db, "select count(*) from minio_direct_file"))
		require.Equal(t, int64(1), queryCount(t, db, "select count(*) from minio_direct_file where id=1"))

		// Replacing the same key between statements checks that a warm cache
		// cannot make the second LOAD observe the previous object generation.
		server.put(t, key, replacement)
		mustExec(t, db, "truncate table minio_direct_file")
		mustExec(t, db, minioLoadSQL(server.endpointURL, server, key, "minio_direct_file", "file", false))
		require.Equal(t, int64(2), queryCount(t, db, "select count(*) from minio_direct_file"))
		require.Equal(t, int64(0), queryCount(t, db, "select count(*) from minio_direct_file where id=1"))
		require.Equal(t, int64(1), queryCount(t, db, "select count(*) from minio_direct_file where id=7"))
	})

	t.Run("DirectStream", func(t *testing.T) {
		const key = "direct/source.stream"
		server.put(t, key, stream)
		mustExec(t, db, "drop table if exists minio_direct_stream")
		mustExec(t, db, "create table minio_direct_stream(id bigint not null, name varchar(50))")
		mustExec(t, db, minioLoadSQL(server.endpointURL, server, key, "minio_direct_stream", "stream", false))
		require.Equal(t, int64(2), queryCount(t, db, "select count(*) from minio_direct_stream"))
		require.Equal(t, int64(1), queryCount(t, db, "select count(*) from minio_direct_stream where id=4 and name='stream-b'"))
	})

	t.Run("S3BackedStage", func(t *testing.T) {
		const key = "stage/source.arrow"
		server.put(t, key, original)
		mustExec(t, db, "drop stage if exists arrow_minio_stage")
		t.Cleanup(func() { _, _ = db.Exec("drop stage if exists arrow_minio_stage") })
		mustExec(t, db, fmt.Sprintf(
			"create stage arrow_minio_stage URL='s3://%s/stage/' CREDENTIALS={"+
				"'AWS_KEY_ID'='%s','AWS_SECRET_KEY'='%s','AWS_REGION'='us-east-1',"+
				"'PROVIDER'='minio','ENDPOINT'='%s'}",
			server.bucket, server.user, server.password, server.endpointURL))
		mustExec(t, db, "drop table if exists minio_stage")
		mustExec(t, db, "create table minio_stage(id bigint not null, name varchar(50))")
		mustExec(t, db,
			"load data infile {'filepath'='stage://arrow_minio_stage/source.arrow','format'='arrow'} into table minio_stage")
		require.Equal(t, int64(2), queryCount(t, db, "select count(*) from minio_stage"))
		mustExec(t, db, "drop stage arrow_minio_stage")
	})

	t.Run("MultiObjectSuccessAndCorruptRollback", func(t *testing.T) {
		part1Path := fixtureIDName(t, t.TempDir(), "part1.arrow", containerFile,
			[][]idNameRow{{{id: 11, name: "part-one"}}})
		part2Path := fixtureIDName(t, t.TempDir(), "part2.arrow", containerFile,
			[][]idNameRow{{{id: 12, name: "part-two"}}})
		server.put(t, "multi-ok/part1.arrow", mustReadFile(t, part1Path))
		server.put(t, "multi-ok/part2.arrow", mustReadFile(t, part2Path))

		mustExec(t, db, "drop table if exists minio_multi_ok")
		mustExec(t, db, "create table minio_multi_ok(id bigint not null, name varchar(50))")
		mustExec(t, db, minioLoadSQL(
			server.endpointURL, server, "multi-ok/part*.arrow", "minio_multi_ok", "file", true))
		require.Equal(t, int64(2), queryCount(t, db, "select count(*) from minio_multi_ok"))
		require.Equal(t, int64(2), queryCount(t, db, "select count(distinct id) from minio_multi_ok"))

		server.put(t, "multi-bad/01-valid.arrow", original)
		server.put(t, "multi-bad/02-corrupt.arrow", []byte("not an Arrow IPC file"))
		mustExec(t, db, "drop table if exists minio_multi_bad")
		mustExec(t, db, "create table minio_multi_bad(id bigint not null, name varchar(50))")
		mustExec(t, db, "insert into minio_multi_bad values (0, 'seed')")
		_, err := db.Exec(minioLoadSQL(
			server.endpointURL, server, "multi-bad/*.arrow", "minio_multi_bad", "file", true))
		require.Error(t, err)
		require.Equal(t, int64(1), queryCount(t, db, "select count(*) from minio_multi_bad"))

		// The next statement is the reuse control for planner/reader cleanup.
		mustExec(t, db, minioLoadSQL(
			server.endpointURL, server, "multi-bad/01-valid.arrow", "minio_multi_bad", "file", false))
		require.Equal(t, int64(3), queryCount(t, db, "select count(*) from minio_multi_bad"))
	})

	t.Run("ObjectChangeFailsClosed", func(t *testing.T) {
		const key = "fault/object-change.arrow"
		server.put(t, key, original)
		mutationDone := make(chan error, 1)
		var mutationStarted atomic.Bool
		proxyEndpoint := startArrowMinIOProxy(t, server.endpointURL,
			func(w http.ResponseWriter, r *http.Request) bool {
				if !isConditionalRangeGET(r) || !mutationStarted.CompareAndSwap(false, true) {
					return false
				}
				mutationCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := server.putObject(mutationCtx, key, replacement); err != nil {
					mutationDone <- err
					http.Error(w, "failed to replace fault-injection object", http.StatusInternalServerError)
					return true
				}
				mutationDone <- nil
				return false
			})

		mustExec(t, db, "drop table if exists minio_object_change")
		mustExec(t, db, "create table minio_object_change(id bigint not null, name varchar(50))")
		mustExec(t, db, "insert into minio_object_change values (0, 'seed')")
		_, err := db.Exec(minioLoadSQL(proxyEndpoint, server, key, "minio_object_change", "file", false))
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "object changed")
		select {
		case mutationErr := <-mutationDone:
			require.NoError(t, mutationErr)
		case <-time.After(5 * time.Second):
			t.Fatal("the fault proxy never observed a conditional range GET")
		}
		require.Equal(t, int64(1), queryCount(t, db, "select count(*) from minio_object_change"))
	})

	t.Run("CanceledRequestReleasesS3Read", func(t *testing.T) {
		const key = "fault/cancel.arrow"
		server.put(t, key, original)
		requestStarted := make(chan struct{})
		requestCanceled := make(chan struct{})
		var blocked atomic.Bool
		proxyEndpoint := startArrowMinIOProxy(t, server.endpointURL,
			func(w http.ResponseWriter, r *http.Request) bool {
				if !isConditionalRangeGET(r) || !blocked.CompareAndSwap(false, true) {
					return false
				}
				close(requestStarted)
				select {
				case <-r.Context().Done():
					close(requestCanceled)
				case <-time.After(30 * time.Second):
					http.Error(w, "timed out waiting for request cancellation", http.StatusGatewayTimeout)
				}
				return true
			})

		mustExec(t, db, "drop table if exists minio_cancel")
		mustExec(t, db, "create table minio_cancel(id bigint not null, name varchar(50))")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errCh := make(chan error, 1)
		go func() {
			_, err := db.ExecContext(ctx, minioLoadSQL(proxyEndpoint, server, key, "minio_cancel", "file", false))
			errCh <- err
		}()
		select {
		case <-requestStarted:
			cancel()
		case <-time.After(30 * time.Second):
			cancel()
			t.Fatal("timed out waiting for the conditional MinIO request")
		}
		select {
		case err := <-errCh:
			require.Error(t, err)
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for the canceled MinIO LOAD")
		}
		select {
		case <-requestCanceled:
		case <-time.After(5 * time.Second):
			t.Fatal("the in-flight MinIO range request did not observe cancellation")
		}
		// go-sql-driver/mysql discards the connection after ExecContext is
		// canceled. Re-establish session-local database state on the replacement
		// connection before checking durable table state.
		mustExec(t, db, "use arrow_bvt")
		require.Equal(t, int64(0), queryCount(t, db, "select count(*) from minio_cancel"))
	})
}

type arrowLoadMinIO struct {
	endpoint    string
	endpointURL string
	bucket      string
	user        string
	password    string
	client      *minio.Client
}

func startArrowLoadMinIO(t *testing.T) arrowLoadMinIO {
	t.Helper()
	executable, err := exec.LookPath("minio")
	if errors.Is(err, exec.ErrNotFound) {
		t.Skip("local MinIO binary is not installed")
	}
	require.NoError(t, err)

	reserveAddress := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		address := listener.Addr().String()
		require.NoError(t, listener.Close())
		return address
	}
	endpoint, consoleEndpoint := reserveAddress(), reserveAddress()
	logPath := filepath.Join(t.TempDir(), "minio.log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)

	const user = "arrowtest"
	const password = "arrowtest-secret"
	command := exec.Command(executable, "server", t.TempDir(), "--address", endpoint, "--console-address", consoleEndpoint)
	command.Env = append(os.Environ(), "MINIO_ROOT_USER="+user, "MINIO_ROOT_PASSWORD="+password)
	command.Stdout = logFile
	command.Stderr = logFile
	require.NoError(t, command.Start())
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_, _ = command.Process.Wait()
		_ = logFile.Close()
	})

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(user, password, ""),
		Region: "us-east-1",
	})
	require.NoError(t, err)
	bucket := "matrixone-arrow-load"
	deadline := time.Now().Add(15 * time.Second)
	for {
		attemptCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		err = client.MakeBucket(attemptCtx, bucket, minio.MakeBucketOptions{Region: "us-east-1"})
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			logBytes, _ := os.ReadFile(logPath)
			t.Fatalf("start local MinIO: %v\n%s", err, logBytes)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return arrowLoadMinIO{
		endpoint: endpoint, endpointURL: "http://" + endpoint,
		bucket: bucket, user: user, password: password, client: client,
	}
}

func (m arrowLoadMinIO) put(t *testing.T, key string, payload []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, m.putObject(ctx, key, payload))
}

func (m arrowLoadMinIO) putObject(ctx context.Context, key string, payload []byte) error {
	_, err := m.client.PutObject(
		ctx, m.bucket, key, bytes.NewReader(payload), int64(len(payload)),
		minio.PutObjectOptions{ContentType: "application/vnd.apache.arrow.file"},
	)
	return err
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	payload, err := os.ReadFile(path)
	require.NoError(t, err)
	return payload
}

func minioLoadSQL(endpoint string, server arrowLoadMinIO, key, table, container string, parallel bool) string {
	containerOption := ""
	if container != "" {
		containerOption = fmt.Sprintf(",'arrow_container'='%s'", container)
	}
	stmt := fmt.Sprintf(
		"load data url s3option {'endpoint'='%s','access_key_id'='%s','secret_access_key'='%s',"+
			"'bucket'='%s','region'='us-east-1','provider'='minio','filepath'='%s','format'='arrow'%s} into table %s",
		endpoint, server.user, server.password, server.bucket, key, containerOption, table)
	if parallel {
		stmt += " parallel 'true'"
	}
	return stmt
}

func isConditionalRangeGET(r *http.Request) bool {
	return r.Method == http.MethodGet && r.Header.Get("Range") != "" && r.Header.Get("If-Match") != ""
}

// startArrowMinIOProxy forwards signed path-style S3 requests to MinIO while
// allowing a test to intercept one protocol phase. interceptor returns true
// only when it has produced the complete response itself.
func startArrowMinIOProxy(
	t *testing.T,
	targetEndpoint string,
	interceptor func(http.ResponseWriter, *http.Request) bool,
) string {
	t.Helper()
	target, err := url.Parse(targetEndpoint)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(target)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if interceptor(w, r) {
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server.URL
}
