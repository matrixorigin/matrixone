// Copyright 2026 Matrix Origin
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

package write

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestLimitedZstd(t *testing.T) {
	c := ParquetOptions{}.compression().(*limitedZstd)
	other := ParquetOptions{}.compression().(*limitedZstd)
	require.NotSame(t, c, other)
	require.Nil(t, c.enc)
	for _, src := range [][]byte{nil, []byte("repeated repeated repeated")} {
		dst := make([]byte, 0, 1024)
		encoded, err := c.Encode(dst, src)
		require.NoError(t, err)
		require.Same(t, &dst[:cap(dst)][0], &encoded[0])
		decoded, err := (&zstd.Codec{}).Decode(nil, encoded)
		require.NoError(t, err)
		require.True(t, bytes.Equal(src, decoded))
	}
	require.Nil(t, other.enc)
	_, err := other.Encode(nil, []byte("other owner"))
	require.NoError(t, err)
	require.NotSame(t, c.enc, other.enc)
}

func TestLimitedZstdConcurrent(t *testing.T) {
	c := ParquetOptions{}.compression().(*limitedZstd)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			src := []byte("concurrent encoder input")
			encoded, err := c.Encode(nil, src)
			if err != nil {
				t.Error(err)
				return
			}
			decoded, err := (&zstd.Codec{}).Decode(nil, encoded)
			if err != nil || !bytes.Equal(src, decoded) {
				t.Errorf("decode: %v, got %q", err, decoded)
			}
		}()
	}
	wg.Wait()
}

// A failed decode must not poison the shared codec, including while other
// callers are using its encoder and decoder.
func TestLimitedZstdDecodeFailureAndReuse(t *testing.T) {
	c := ParquetOptions{}.compression().(*limitedZstd)
	payload := bytes.Repeat([]byte("window boundary and reuse"), 8000)
	encoded, err := c.Encode(nil, payload)
	require.NoError(t, err)
	_, err = c.Decode(nil, []byte("not a zstd frame"))
	require.Error(t, err)
	decoded, err := c.Decode(nil, encoded)
	require.NoError(t, err)
	require.Equal(t, payload, decoded)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			src := bytes.Repeat([]byte{byte(i), byte(i + 1), byte(i + 2)}, 50000+i)
			compressed, err := c.Encode(nil, src)
			if err != nil {
				t.Errorf("encode %d: %v", i, err)
				return
			}
			if _, err := c.Decode(nil, []byte("bad frame")); err == nil {
				t.Errorf("decode %d: expected error", i)
				return
			}
			got, err := c.Decode(nil, compressed)
			if err != nil || !bytes.Equal(src, got) {
				t.Errorf("round trip %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestFanoutZstdOwnerAcrossRotation(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"part"})
	v := vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 10; i++ {
		require.NoError(t, vector.AppendFixed[int32](v, int32(i), false, mp))
	}
	bat.Vecs[0] = v
	bat.SetRowCount(10)
	defer bat.Clean(mp)
	factory := &memoryDataFileFactory{objects: make(map[string]*bytes.Buffer)}
	cfg := FanoutWriterConfig{Schema: api.Schema{Fields: []api.SchemaField{{ID: 1, Name: "part", Type: api.IcebergType{Kind: api.TypeInt}}}}, PartitionSpec: api.PartitionSpec{Fields: []api.PartitionField{{SourceID: 1, Name: "part"}}}, TableLocation: "s3://test/table"}
	w, err := NewFanoutParquetDataWriter(ctx, cfg, factory)
	require.NoError(t, err)
	other, err := NewFanoutParquetDataWriter(ctx, cfg, factory)
	require.NoError(t, err)
	codec := w.codec.(*limitedZstd)
	require.NotSame(t, codec, other.codec)
	require.NoError(t, w.WriteBatch(ctx, bat.Attrs, bat))
	require.LessOrEqual(t, len(w.active), 8)
	require.Same(t, codec, w.codec)
	require.NotNil(t, codec.enc) // Eviction closes files and initializes the shared encoder.
	enc := codec.enc
	files, err := w.Close(ctx)
	require.NoError(t, err)
	require.Len(t, files, 10)
	require.Same(t, enc, codec.enc)
	require.NoError(t, other.Abort(ctx))
}
