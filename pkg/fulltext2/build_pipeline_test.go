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

package fulltext2

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

var pipelineIncludeOpt = WithIncludeTypes([]int32{int32(types.T_int64)})

type pipelineSeg struct {
	id    string
	bytes []byte
}

func pipelineTokenize(content []byte) ([]WordPos, error) {
	tok, err := DocTokenizer(ParserDefault)
	if err != nil {
		return nil, err
	}
	var terms []WordPos
	for t, terr := range tok.Tokenize(content) {
		if terr != nil {
			return nil, terr
		}
		slen := t.TokenBytes[0]
		terms = append(terms, WordPos{Word: string(t.TokenBytes[1 : slen+1]), Pos: t.BytePos})
	}
	return terms, nil
}

// pipelineCorpus returns n source rows: most carry text, every 7th carries parser-made terms,
// every 11th is empty, and every 3rd carries INCLUDE values.
func pipelineCorpus(n int, varcharPk bool) []BuildDoc {
	vocab := []string{"apple", "banana", "cherry", "delta", "echo", "fox", "golf", "hotel", "india", "juliet"}
	docs := make([]BuildDoc, 0, n)
	for i := 0; i < n; i++ {
		var pk any = int64(i + 1)
		if varcharPk {
			pk = []byte(fmt.Sprintf("pk-%05d", i+1))
		}
		doc := BuildDoc{PK: pk}
		switch {
		case i%11 == 0:
			doc.Content = []byte{}
		case i%7 == 0:
			doc.Terms = []WordPos{{Word: vocab[i%len(vocab)], Pos: 0}, {Word: "tuple", Pos: 1}}
		default:
			text := ""
			for j := 0; j <= i%9; j++ {
				text += vocab[(i*j+j)%len(vocab)] + " "
			}
			doc.Content = []byte(text)
		}
		if i%3 == 0 {
			doc.Include = []any{int64(i)}
			doc.HasInclude = true
		}
		docs = append(docs, doc)
	}
	return docs
}

func cloneBuildDoc(d BuildDoc) BuildDoc {
	c := d
	if d.Content != nil {
		c.Content = append([]byte{}, d.Content...)
	}
	if d.Terms != nil {
		c.Terms = append([]WordPos(nil), d.Terms...)
	}
	return c
}

// serialPipelineReference builds the corpus with the serial create loop: tokenize, Add,
// SetInclude, and seal + Finish whenever ReachedSegmentCap holds.
func serialPipelineReference(t *testing.T, docs []BuildDoc, pkType int32, docCap, postingCap int64) []pipelineSeg {
	var out []pipelineSeg
	cur := NewBuilder("uid", pkType, pipelineIncludeOpt)
	seal := func() {
		if cur.NumDocs() == 0 {
			return
		}
		seg, err := cur.Finish()
		require.NoError(t, err)
		seg.Id = SubIndexId("uid", len(out))
		buf, err := seg.Serialize()
		require.NoError(t, err)
		out = append(out, pipelineSeg{id: seg.Id, bytes: buf})
		cur = NewBuilder("uid", pkType, pipelineIncludeOpt)
	}
	for _, d := range docs {
		terms := d.Terms
		if d.Content != nil {
			var err error
			terms, err = pipelineTokenize(d.Content)
			require.NoError(t, err)
		}
		if len(terms) == 0 {
			continue
		}
		for _, w := range terms {
			require.NoError(t, cur.Add(w.Word, w.Pos, d.PK))
		}
		if d.HasInclude {
			cur.SetInclude(d.PK, d.Include)
		}
		if ReachedSegmentCap(cur, docCap, postingCap) {
			seal()
		}
	}
	seal()
	return out
}

func newTestPipeline(t *testing.T, threads, maxFinishing int, pkType int32, docCap, postingCap int64,
	dir string, write func(*SpilledSegment) error) *BuildPipeline {
	return NewBuildPipeline(BuildPipelineConfig{
		Threads:      threads,
		MaxFinishing: maxFinishing,
		Capacity:     docCap,
		PostingCap:   postingCap,
		NewBuilder:   func() *Builder { return NewBuilder("uid", pkType, pipelineIncludeOpt) },
		SegmentID:    func(i int) string { return SubIndexId("uid", i) },
		Tokenize:     pipelineTokenize,
		SpillDir:     dir,
		Write:        write,
	})
}

func spilledBytes(t *testing.T, s *SpilledSegment) []byte {
	buf := make([]byte, s.size)
	_, err := s.fp.ReadAt(buf, 0)
	require.NoError(t, err)
	return buf
}

func requireEmptyDir(t *testing.T, dir string) {
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "spill files must be removed")
}

func TestBuildPipelineMatchesSerialBuild(t *testing.T) {
	for _, varcharPk := range []bool{false, true} {
		pkType := int32(types.T_int64)
		if varcharPk {
			pkType = int32(types.T_varchar)
		}
		docs := pipelineCorpus(1500, varcharPk)
		for _, caps := range [][2]int64{{37, 0}, {0, 300}, {0, 0}} {
			want := serialPipelineReference(t, docs, pkType, caps[0], caps[1])
			for _, threads := range []int{1, 2, 4, 8} {
				for _, maxFinishing := range []int{1, 2} {
					name := fmt.Sprintf("varchar=%v/caps=%v/threads=%d/finishing=%d", varcharPk, caps, threads, maxFinishing)
					t.Run(name, func(t *testing.T) {
						dir := t.TempDir()
						var got []pipelineSeg
						p := newTestPipeline(t, threads, maxFinishing, pkType, caps[0], caps[1], dir, func(s *SpilledSegment) error {
							got = append(got, pipelineSeg{id: s.Id, bytes: spilledBytes(t, s)})
							return nil
						})
						for _, d := range docs {
							require.NoError(t, p.Add(cloneBuildDoc(d)))
						}
						n, err := p.Close()
						require.NoError(t, err)
						require.Equal(t, len(want), n)
						require.Equal(t, len(want), len(got))
						for i := range want {
							require.Equal(t, want[i].id, got[i].id)
							require.Equal(t, want[i].bytes, got[i].bytes, "segment %d differs", i)
						}
						requireEmptyDir(t, dir)
					})
				}
			}
		}
	}
}

func TestBuildPipelineEmpty(t *testing.T) {
	dir := t.TempDir()
	p := newTestPipeline(t, 4, 2, int32(types.T_int64), 0, 0, dir, func(*SpilledSegment) error {
		t.Fatal("no segment expected")
		return nil
	})
	require.NoError(t, p.Add(BuildDoc{PK: int64(1), Content: []byte{}}))
	n, err := p.Close()
	require.NoError(t, err)
	require.Zero(t, n)
	requireEmptyDir(t, dir)
}

func TestBuildPipelineTokenizeError(t *testing.T) {
	dir := t.TempDir()
	boom := errors.New("tokenize failed")
	p := NewBuildPipeline(BuildPipelineConfig{
		Threads:      4,
		MaxFinishing: 2,
		Capacity:     10,
		NewBuilder:   func() *Builder { return NewBuilder("uid", int32(types.T_int64)) },
		SegmentID:    func(i int) string { return SubIndexId("uid", i) },
		Tokenize: func(content []byte) ([]WordPos, error) {
			if string(content) == "bad" {
				return nil, boom
			}
			return pipelineTokenize(content)
		},
		SpillDir: dir,
		Write:    func(*SpilledSegment) error { return nil },
	})
	var err error
	for i := 0; i < 600 && err == nil; i++ {
		content := []byte("alpha beta gamma")
		if i == 500 {
			content = []byte("bad")
		}
		err = p.Add(BuildDoc{PK: int64(i + 1), Content: content})
	}
	if err == nil {
		_, err = p.Close()
	}
	require.ErrorIs(t, err, boom)
	p.Abort()
	requireEmptyDir(t, dir)
}

func TestBuildPipelineWriteErrorAbortRemovesSpills(t *testing.T) {
	dir := t.TempDir()
	boom := errors.New("write failed")
	writes := 0
	p := newTestPipeline(t, 4, 2, int32(types.T_int64), 20, 0, dir, func(*SpilledSegment) error {
		writes++
		if writes == 2 {
			return boom
		}
		return nil
	})
	var err error
	for _, d := range pipelineCorpus(800, false) {
		if err = p.Add(d); err != nil {
			break
		}
	}
	if err == nil {
		_, err = p.Close()
	}
	require.ErrorIs(t, err, boom)
	p.Abort()
	p.Abort()
	requireEmptyDir(t, dir)
}

func TestBuildPipelineTokenizePanicReRaisedOnOwner(t *testing.T) {
	dir := t.TempDir()
	p := NewBuildPipeline(BuildPipelineConfig{
		Threads:    4,
		NewBuilder: func() *Builder { return NewBuilder("uid", int32(types.T_int64)) },
		SegmentID:  func(i int) string { return SubIndexId("uid", i) },
		Tokenize: func(content []byte) ([]WordPos, error) {
			if string(content) == "panic" {
				panic("tokenizer panic")
			}
			return pipelineTokenize(content)
		},
		SpillDir: dir,
		Write:    func(*SpilledSegment) error { return nil },
	})
	require.NoError(t, p.Add(BuildDoc{PK: int64(1), Content: []byte("panic")}))
	require.PanicsWithValue(t, "tokenizer panic", func() { _, _ = p.Close() })
	p.Abort()
	requireEmptyDir(t, dir)
}

func TestBuildPipelineSpillErrorSurfaces(t *testing.T) {
	missing := t.TempDir() + "/missing"
	p := newTestPipeline(t, 4, 2, int32(types.T_int64), 20, 0, missing, func(*SpilledSegment) error {
		t.Fatal("a failed spill must not be written")
		return nil
	})
	var err error
	for _, d := range pipelineCorpus(300, false) {
		if err = p.Add(d); err != nil {
			break
		}
	}
	if err == nil {
		_, err = p.Close()
	}
	require.Error(t, err)
	p.Abort()
}

func TestBuildPipelineCloseIsTerminal(t *testing.T) {
	dir := t.TempDir()
	writes := 0
	p := newTestPipeline(t, 2, 1, int32(types.T_int64), 0, 0, dir, func(*SpilledSegment) error {
		writes++
		return nil
	})
	require.NoError(t, p.Add(BuildDoc{PK: int64(1), Content: []byte("alpha beta")}))
	n, err := p.Close()
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = p.Close()
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Error(t, p.Add(BuildDoc{PK: int64(2), Content: []byte("gamma")}))
	require.Equal(t, 1, writes)
	requireEmptyDir(t, dir)
}

func TestBuildPipelineWorkerAndChunkBounds(t *testing.T) {
	p := newTestPipeline(t, 1024, 2, int32(types.T_int64), 0, 0, t.TempDir(), func(*SpilledSegment) error { return nil })
	require.LessOrEqual(t, p.cfg.Threads, runtime.GOMAXPROCS(0))
	require.LessOrEqual(t, p.maxBytes, buildChunkMaxBytes)
	require.LessOrEqual(t, p.maxRows, buildChunkMaxRows)
	small := newTestPipeline(t, 1, 1, int32(types.T_int64), 0, 0, t.TempDir(), func(*SpilledSegment) error { return nil })
	require.Equal(t, buildChunkBytesPerThread, small.maxBytes)
	require.Equal(t, buildChunkRowsPerThread, small.maxRows)
}

func TestBuildPipelineWritePanicWaitsForTokenizers(t *testing.T) {
	dir := t.TempDir()
	var running atomic.Int32
	p := NewBuildPipeline(BuildPipelineConfig{
		Threads:      2,
		MaxFinishing: 1 << 20,
		Capacity:     1,
		NewBuilder:   func() *Builder { return NewBuilder("uid", int32(types.T_int64)) },
		SegmentID:    func(i int) string { return SubIndexId("uid", i) },
		Tokenize: func(content []byte) ([]WordPos, error) {
			running.Add(1)
			defer running.Add(-1)
			time.Sleep(time.Millisecond)
			return pipelineTokenize(content)
		},
		SpillDir: dir,
		Write:    func(*SpilledSegment) error { panic("write panic") },
	})
	rows := p.maxRows
	for i := 0; i < rows; i++ {
		require.NoError(t, p.Add(BuildDoc{PK: int64(i + 1), Content: []byte("alpha beta")}))
	}
	require.NotEmpty(t, p.jobs)
	for _, j := range p.jobs {
		<-j.done
	}
	require.PanicsWithValue(t, "write panic", func() {
		for i := 0; i < rows; i++ {
			_ = p.Add(BuildDoc{PK: int64(rows + i + 1), Content: []byte("gamma delta")})
		}
	})
	require.Zero(t, running.Load(), "no tokenizer may outlive the panicking call")
	p.Abort()
	requireEmptyDir(t, dir)
}

func TestBuildPipelineDirectTermsCountTowardChunkBytes(t *testing.T) {
	p := newTestPipeline(t, 1, 1, int32(types.T_int64), 0, 0, t.TempDir(), func(*SpilledSegment) error { return nil })
	defer p.Abort()
	big := string(make([]byte, 1<<20))
	for i := 0; i < 2; i++ {
		require.NoError(t, p.Add(BuildDoc{PK: int64(i + 1), Terms: []WordPos{{Word: big, Pos: 0}}}))
		require.Less(t, p.chunkBytes, p.maxBytes)
	}
	require.Empty(t, p.chunk, "terms-only rows must flush on the byte budget")

	inc := BuildDoc{PK: int64(3), Content: []byte("a"), Include: []any{make([]byte, 1<<20), int64(1)}, HasInclude: true}
	require.Equal(t, 1+1<<20, buildDocBytes(&inc))
}
