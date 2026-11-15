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

package fileservice

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type replayCountingWriter struct {
	bytes.Buffer
	calls      int
	maxPerCall int
	cancel     context.CancelFunc
}

func (w *replayCountingWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.maxPerCall > 0 && len(p) > w.maxPerCall {
		p = p[:w.maxPerCall]
	}
	n, err := w.Buffer.Write(p)
	if w.cancel != nil {
		w.cancel()
	}
	return n, err
}

func TestReplayDataWriterToLargeAndFragmented(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		size, count, maxCalls int
	}{
		{"large span", 1 << 20, 1, 1},
		{"many small spans", 160 << 10, 10000, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := bytes.Repeat([]byte("x"), tc.size)
			entries := make([]IOEntry, 0, tc.count)
			for i := 0; i < tc.count; i++ {
				lo := len(data) * i / tc.count
				hi := len(data) * (i + 1) / tc.count
				entries = append(entries, IOEntry{Offset: int64(lo), Size: int64(hi - lo), Data: data[lo:hi]})
			}
			reader := &replayDataReader{ctx: context.Background(), entries: entries, remaining: int64(len(data))}
			sink := new(replayCountingWriter)
			n, err := io.Copy(sink, io.NopCloser(reader))
			require.NoError(t, err)
			require.EqualValues(t, len(data), n)
			require.Equal(t, data, sink.Bytes())
			require.LessOrEqual(t, sink.calls, tc.maxCalls)
		})
	}
}

func TestReplayDataWriterToShortWriteAndCancel(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 64<<10)
	entries := []IOEntry{{Offset: 0, Size: int64(len(data)), Data: data}, {Offset: int64(len(data)), Size: int64(len(data)), Data: data}}
	t.Run("short write", func(t *testing.T) {
		reader := &replayDataReader{ctx: context.Background(), entries: entries, remaining: int64(2 * len(data))}
		sink := &replayCountingWriter{maxPerCall: 7}
		n, err := io.Copy(sink, io.NopCloser(reader))
		require.ErrorIs(t, err, io.ErrShortWrite)
		require.EqualValues(t, 7, n)
		require.Equal(t, data[:7], sink.Bytes())
	})
	t.Run("cancellation after first span", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reader := &replayDataReader{ctx: ctx, entries: entries, remaining: int64(2 * len(data))}
		sink := &replayCountingWriter{cancel: cancel}
		n, err := io.Copy(sink, io.NopCloser(reader))
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, len(data), n)
		require.Equal(t, data, sink.Bytes())
	})
}

type replayPartialErrorWriter struct {
	bytes.Buffer
	err error
}

func (w *replayPartialErrorWriter) Write(p []byte) (int, error) {
	n, _ := w.Buffer.Write(p[:min(len(p), 3)])
	return n, w.err
}

func TestReplayDataWriterToBufferedErrorAndLimit(t *testing.T) {
	entries := []IOEntry{
		{Offset: 0, Size: 4, Data: []byte("abcd")},
		{Offset: 4, Size: 4, Data: []byte("efgh")},
	}
	t.Run("partial error from buffered write", func(t *testing.T) {
		sentinel := errors.New("write failed")
		sink := &replayPartialErrorWriter{err: sentinel}
		reader := &replayDataReader{ctx: context.Background(), entries: entries, remaining: 8}
		n, err := io.Copy(sink, io.NopCloser(reader))
		require.ErrorIs(t, err, sentinel)
		require.EqualValues(t, 3, n)
		require.Equal(t, "abc", sink.String())
	})
	t.Run("replay only consumed prefix", func(t *testing.T) {
		sink := new(replayCountingWriter)
		reader := &replayDataReader{ctx: context.Background(), entries: entries, remaining: 6}
		n, err := io.Copy(sink, io.NopCloser(reader))
		require.NoError(t, err)
		require.EqualValues(t, 6, n)
		require.Equal(t, "abcdef", sink.String())
	})
}
