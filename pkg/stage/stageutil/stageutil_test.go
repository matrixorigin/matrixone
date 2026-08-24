// Copyright 2024 Matrix Origin
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

package stageutil

import (
	"context"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestStageCache(t *testing.T) {

	proc := testutil.NewProcess(t)
	cache := proc.GetStageCache()

	credentials := make(map[string]string)
	credentials["aws_region"] = "region"
	credentials["aws_id"] = "id"
	credentials["aws_secret"] = "secret"

	rsu, err := url.Parse("file:///tmp")
	require.Nil(t, err)
	subu, err := url.Parse("stage://rsstage/substage")
	require.Nil(t, err)

	cache.Set("rsstage", stage.StageDef{Id: 1, Name: "rsstage", Url: rsu, Credentials: credentials})
	cache.Set("substage", stage.StageDef{Id: 1, Name: "ftstage", Url: subu})

	// get the final URL totally based on cache value
	s, err := UrlToStageDef("stage://substage/a.csv", proc)
	require.Nil(t, err)

	require.Equal(t, s.Url.String(), "file:///tmp/substage/a.csv")
	require.Equal(t, s.Credentials["aws_region"], "region")
	require.Equal(t, s.Credentials["aws_id"], "id")
	require.Equal(t, s.Credentials["aws_secret"], "secret")

	// change the local stagedef does not change the cache
	s.Url, err = url.Parse("https://localhost/path")
	require.Nil(t, err)

	// preserve the cache
	rs, ok := cache.Get("rsstage")
	require.True(t, ok)
	require.Equal(t, rs.Url.String(), "file:///tmp")
	ss, ok := cache.Get("substage")
	require.True(t, ok)
	require.Equal(t, ss.Url.String(), "stage://rsstage/substage")
}

func TestStageFail(t *testing.T) {

	proc := testutil.NewProcess(t)
	_, err := UrlToStageDef("stage:///path", proc)
	require.NotNil(t, err)

	u, err := url.Parse("stage:///path")
	require.Nil(t, err)
	s := stage.StageDef{Id: 1, Name: "rsstage", Url: u}
	_, err = ExpandSubStage(s, proc)
	require.NotNil(t, err)

	_, err = UrlToStageDef("not a url", proc)
	require.NotNil(t, err)
}

// setStage seeds the process stage cache so resolution runs entirely off the cache — the same
// state a repeated lookup reaches in production, and the state in which a cycle spins without
// issuing any SQL.
func setStage(t *testing.T, proc *process.Process, name, rawurl string) {
	u, err := url.Parse(rawurl)
	require.NoError(t, err)
	proc.GetStageCache().Set(name, stage.StageDef{Id: 1, Name: name, Url: u})
}

// TestStageCyclicReference: a stage:// cycle has no concrete URL to resolve to, so resolution must
// fail with a bounded error instead of walking the cycle forever (#26890). Self-cycle, two-node and
// three-node cycles are all reached through the normal UrlToStageDef entry point.
func TestStageCyclicReference(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stages map[string]string
		furl   string
	}{
		{
			name:   "self",
			stages: map[string]string{"selfa": "stage://selfa/loop"},
			furl:   "stage://selfa/a.csv",
		},
		{
			name:   "two-node",
			stages: map[string]string{"twoa": "stage://twob/x", "twob": "stage://twoa/y"},
			furl:   "stage://twoa/a.csv",
		},
		{
			name: "three-node",
			stages: map[string]string{
				"tria": "stage://trib/x", "trib": "stage://tric/y", "tric": "stage://tria/z",
			},
			furl: "stage://tria/a.csv",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			for name, u := range tc.stages {
				setStage(t, proc, name, u)
			}

			done := make(chan error, 1)
			go func() {
				_, err := UrlToStageDef(tc.furl, proc)
				done <- err
			}()

			select {
			case err := <-done:
				require.Error(t, err)
				require.Contains(t, err.Error(), "cyclic stage reference")
			case <-time.After(30 * time.Second):
				t.Fatal("UrlToStageDef hung on a cyclic stage reference")
			}
		})
	}
}

// TestStageChainNoFalseCycle: a legal multi-hop chain that revisits no stage must still resolve —
// the cycle check must not reject repeated path segments or a long acyclic chain.
func TestStageChainNoFalseCycle(t *testing.T) {
	proc := testutil.NewProcess(t)
	setStage(t, proc, "chaina", "stage://chainb/one")
	setStage(t, proc, "chainb", "stage://chainc/one")
	setStage(t, proc, "chainc", "file:///tmp")

	s, err := UrlToStageDef("stage://chaina/a.csv", proc)
	require.NoError(t, err)
	require.Equal(t, "file:///tmp/one/one/a.csv", s.Url.String())
}

func Test_runSql(t *testing.T) {
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, nil
	}))

	proc := testutil.NewProcess(t)
	_, err := runSql(proc, "")
	require.Nil(t, err)
}

func TestDeleteStageFiles(t *testing.T) {
	proc := testutil.NewProcess(t)
	cache := proc.GetStageCache()

	dir := t.TempDir()
	stageURL := &url.URL{Scheme: stage.FILE_PROTOCOL, Path: dir}
	cache.Set("mystage", stage.StageDef{Id: 1, Name: "mystage", Url: stageURL})

	fileA := filepath.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(fileA, []byte("a"), 0600))

	deleted, err := DeleteStageFiles(context.Background(), proc, "stage://mystage/a.txt", false)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	_, err = os.Stat(fileA)
	require.True(t, os.IsNotExist(err))

	fileB := filepath.Join(dir, "b.txt")
	fileC := filepath.Join(dir, "c.log")
	require.NoError(t, os.WriteFile(fileB, []byte("b"), 0600))
	require.NoError(t, os.WriteFile(fileC, []byte("c"), 0600))

	deleted, err = DeleteStageFiles(context.Background(), proc, "stage://mystage/*.txt", false)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	_, err = os.Stat(fileB)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(fileC)
	require.NoError(t, err)

	deleted, err = DeleteStageFiles(context.Background(), proc, "stage://mystage/*.missing", true)
	require.NoError(t, err)
	require.Equal(t, 0, deleted)

	_, err = DeleteStageFiles(context.Background(), proc, "stage://mystage/*.missing", false)
	require.Error(t, err)
}

func TestDeleteStageFilesErrors(t *testing.T) {
	proc := testutil.NewProcess(t)
	cache := proc.GetStageCache()

	dir := t.TempDir()
	stageURL := &url.URL{Scheme: stage.FILE_PROTOCOL, Path: dir}
	cache.Set("mystage", stage.StageDef{Id: 1, Name: "mystage", Url: stageURL})

	_, err := DeleteStageFiles(context.Background(), proc, "", false)
	require.Error(t, err)

	_, err = DeleteStageFiles(context.Background(), proc, "stage://mystage/a.txt?version=1", false)
	require.Error(t, err)

	deleted, err := DeleteStageFiles(context.Background(), proc, "stage://mystage/missing.txt", true)
	require.NoError(t, err)
	require.Equal(t, 0, deleted)

	_, err = DeleteStageFiles(context.Background(), proc, "stage://mystage/missing.txt", false)
	require.Error(t, err)

	cache.Set("emptystage", stage.StageDef{Id: 2, Name: "emptystage", Url: &url.URL{Scheme: stage.FILE_PROTOCOL}})
	_, err = DeleteStageFiles(context.Background(), proc, "stage://emptystage", false)
	require.Error(t, err)
}

func TestStageListWithPatternNoWildcard(t *testing.T) {
	proc := testutil.NewProcess(t)
	dir := t.TempDir()
	filePath := filepath.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("a"), 0600))

	list, err := StageListWithPattern("", dir, proc)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{path.Join(dir, "a.txt")}, list)
}
