// Copyright 2021 - 2026 Matrix Origin
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

package logservice

import (
	"io"
	"math"
	"strings"
	"testing"

	"github.com/lni/dragonboat/v4/raftpb"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

// An indexed record that reads as EOF must not make the configured LogDB spin.
// Bound opens in the fake FS so an old dependency fails without a hung test.
type eofLogFS struct {
	vfs.FS
	readEOF bool
	opens   int
}

func (fs *eofLogFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	if fs.readEOF && strings.HasSuffix(name, ".log") {
		fs.opens++
		if fs.opens > 1 {
			return nil, vfs.ErrInjected
		}
		f, err := fs.FS.Open(name, opts...)
		if err != nil {
			return nil, err
		}
		return eofLogFile{File: f}, nil
	}
	return fs.FS.Open(name, opts...)
}

type eofLogFile struct{ vfs.File }

func (eofLogFile) Read([]byte) (int, error) { return 0, io.EOF }

func TestTanIndexedEOFReturnsError(t *testing.T) {
	fs := &eofLogFS{FS: vfs.NewMem()}
	t.Cleanup(func() { vfs.ReportLeakedFD(fs.FS, t) })
	cfg := DefaultConfig()
	cfg.FS = fs
	nhConfig := getNodeHostConfig(cfg, nil)
	require.NoError(t, nhConfig.Prepare())
	ldb, err := nhConfig.Expert.LogDBFactory.Create(nhConfig, nil, []string{"tan-eof-test"}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ldb.Close()) })
	want := []raftpb.Entry{{Index: 1, Term: 1, Cmd: []byte("durable entry")}}
	require.NoError(t, ldb.SaveRaftState([]raftpb.Update{{
		ShardID: 1, ReplicaID: 1,
		State:         raftpb.State{Term: 1, Commit: 1},
		EntriesToSave: want,
	}}, 1))
	entries, _, err := ldb.IterateEntries(nil, 0, 1, 1, 1, 2, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, want, entries)

	fs.readEOF = true
	entries, size, err := ldb.IterateEntries(nil, 0, 1, 1, 1, 2, math.MaxUint64)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no progress")
	require.Equal(t, 1, fs.opens, "the same indexed position must not be reopened")
	require.Nil(t, entries)
	require.Zero(t, size)

	fs.readEOF = false
	entries, _, err = ldb.IterateEntries(nil, 0, 1, 1, 1, 2, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, want, entries, "an unsuccessful read must not rewrite or skip stored data")
}
