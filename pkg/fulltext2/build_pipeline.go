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
	"runtime"
	"sync"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	buildChunkBytesPerThread = 2 << 20
	buildChunkRowsPerThread  = 64
	buildChunkMaxBytes       = 16 << 20
	buildChunkMaxRows        = 512
)

// BuildDoc is one source row queued for a pipelined build. Content is the text to tokenize;
// when the parser produces terms directly, Content is nil and Terms holds them. All fields
// must be owned by the BuildDoc (no views into reused vectors).
type BuildDoc struct {
	PK         any
	Content    []byte
	Terms      []WordPos
	Include    []any
	HasInclude bool
}

// BuildPipelineConfig configures a BuildPipeline.
type BuildPipelineConfig struct {
	// Threads is the number of tokenizer workers.
	Threads int
	// MaxFinishing bounds the segments being finished and spilled at once; each holds a
	// full segment's postings in memory.
	MaxFinishing int
	// Capacity and PostingCap are the per-segment doc and posting caps (ReachedSegmentCap).
	Capacity   int64
	PostingCap int64
	// NewBuilder returns an empty builder for the next segment.
	NewBuilder func() *Builder
	// SegmentID names the i-th sealed segment.
	SegmentID func(i int) string
	// Tokenize turns a BuildDoc's Content into terms. It runs on worker goroutines.
	Tokenize func(content []byte) ([]WordPos, error)
	// SpillDir is the directory for spilled segments ("" = the OS temp dir).
	SpillDir string
	// Write persists one spilled segment. It runs on the owner goroutine, in segment order.
	Write func(seg *SpilledSegment) error
}

type buildSegJob struct {
	done     chan struct{}
	observed bool
	spilled  *SpilledSegment
	err      error
	panicVal any
}

// BuildPipeline builds tag=0 segments from source rows with parallel tokenization and
// background segment finishing. Feeding, sealing and writing happen on the owner goroutine
// in source order, so the produced segments are identical to a serial build. All methods
// must be called from the owner goroutine.
type BuildPipeline struct {
	cfg        BuildPipelineConfig
	cur        *Builder
	chunk      []BuildDoc
	chunkBytes int
	maxBytes   int
	maxRows    int
	sealed     int
	written    int
	jobs       []*buildSegJob
	finishing  int
	closed     bool
}

// NewBuildPipeline returns a pipeline for cfg.
func NewBuildPipeline(cfg BuildPipelineConfig) *BuildPipeline {
	if cfg.Threads < 1 {
		cfg.Threads = 1
	}
	if n := runtime.GOMAXPROCS(0); cfg.Threads > n {
		cfg.Threads = n
	}
	if cfg.MaxFinishing < 1 {
		cfg.MaxFinishing = 1
	}
	return &BuildPipeline{
		cfg:      cfg,
		cur:      cfg.NewBuilder(),
		maxBytes: min(cfg.Threads*buildChunkBytesPerThread, buildChunkMaxBytes),
		maxRows:  min(cfg.Threads*buildChunkRowsPerThread, buildChunkMaxRows),
	}
}

// Add queues one source row.
func (p *BuildPipeline) Add(doc BuildDoc) error {
	if p.closed {
		return moerr.NewInternalErrorNoCtx("fulltext2 build pipeline: add after close")
	}
	p.chunk = append(p.chunk, doc)
	p.chunkBytes += buildDocBytes(&doc)
	if p.chunkBytes >= p.maxBytes || len(p.chunk) >= p.maxRows {
		return p.processChunk()
	}
	return nil
}

// Close flushes the queued rows, seals the open segment and writes every pending segment.
// It returns the number of segments written.
func (p *BuildPipeline) Close() (int, error) {
	if p.closed {
		return p.written, nil
	}
	if err := p.processChunk(); err != nil {
		return p.written, err
	}
	if err := p.seal(); err != nil {
		return p.written, err
	}
	p.closed = true
	for len(p.jobs) > 0 {
		if err := p.writeHead(true); err != nil {
			return p.written, err
		}
	}
	return p.written, nil
}

// Abort stops the pipeline after a failure: it waits for running finishers and removes
// every spilled segment that was not written. It is idempotent.
func (p *BuildPipeline) Abort() {
	p.closed = true
	for _, j := range p.jobs {
		<-j.done
		j.spilled.Discard()
	}
	p.jobs = nil
	p.finishing = 0
	p.chunk = nil
	p.cur = nil
}

// buildDocBytes returns the variable-sized payload a queued BuildDoc owns.
func buildDocBytes(doc *BuildDoc) int {
	n := len(doc.Content) + len(doc.Terms)*int(unsafe.Sizeof(WordPos{}))
	for i := range doc.Terms {
		n += len(doc.Terms[i].Word)
	}
	for _, v := range doc.Include {
		switch x := v.(type) {
		case []byte:
			n += len(x)
		case string:
			n += len(x)
		}
	}
	return n
}

func (p *BuildPipeline) processChunk() error {
	docs := p.chunk
	p.chunk = nil
	p.chunkBytes = 0
	if len(docs) == 0 {
		return nil
	}
	errs := make([]error, len(docs))
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		next     int
		panicVal any
	)
	workers := min(p.cfg.Threads, len(docs))
	defer wg.Wait()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					if panicVal == nil {
						panicVal = r
					}
					mu.Unlock()
				}
			}()
			for {
				mu.Lock()
				i := next
				next++
				mu.Unlock()
				if i >= len(docs) {
					return
				}
				if docs[i].Content != nil {
					docs[i].Terms, errs[i] = p.cfg.Tokenize(docs[i].Content)
					docs[i].Content = nil
				}
			}
		}()
	}
	var writeErr error
	for writeErr == nil && len(p.jobs) > 0 && p.jobs[0].isDone() {
		writeErr = p.writeHead(false)
	}
	wg.Wait()
	if panicVal != nil {
		panic(panicVal)
	}
	if writeErr != nil {
		return writeErr
	}
	for i := range docs {
		if errs[i] != nil {
			return errs[i]
		}
		if err := p.feed(&docs[i]); err != nil {
			return err
		}
	}
	return nil
}

func (p *BuildPipeline) feed(doc *BuildDoc) error {
	if len(doc.Terms) == 0 {
		return nil
	}
	for _, w := range doc.Terms {
		if err := p.cur.Add(w.Word, w.Pos, doc.PK); err != nil {
			return err
		}
	}
	if doc.HasInclude {
		p.cur.SetInclude(doc.PK, doc.Include)
	}
	if ReachedSegmentCap(p.cur, p.cfg.Capacity, p.cfg.PostingCap) {
		return p.seal()
	}
	return nil
}

func (p *BuildPipeline) seal() error {
	b := p.cur
	p.cur = p.cfg.NewBuilder()
	if b == nil || b.NumDocs() == 0 {
		return nil
	}
	id := p.cfg.SegmentID(p.sealed)
	p.sealed++
	for p.finishing >= p.cfg.MaxFinishing {
		if err := p.waitOldestFinishing(); err != nil {
			return err
		}
	}
	job := &buildSegJob{done: make(chan struct{})}
	p.jobs = append(p.jobs, job)
	p.finishing++
	dir := p.cfg.SpillDir
	go func() {
		defer close(job.done)
		defer func() {
			if r := recover(); r != nil {
				job.panicVal = r
			}
		}()
		seg, err := b.Finish()
		b = nil
		if err != nil {
			job.err = err
			return
		}
		seg.Id = id
		job.spilled, job.err = seg.Spill(dir)
	}()
	return nil
}

// waitOldestFinishing blocks until the oldest unobserved finisher completes, writing every
// segment that becomes ready at the head of the queue.
func (p *BuildPipeline) waitOldestFinishing() error {
	for _, j := range p.jobs {
		if !j.observed {
			<-j.done
			p.observe(j)
			break
		}
	}
	for len(p.jobs) > 0 && p.jobs[0].isDone() {
		if err := p.writeHead(false); err != nil {
			return err
		}
	}
	return nil
}

func (p *BuildPipeline) observe(j *buildSegJob) {
	if !j.observed {
		j.observed = true
		p.finishing--
	}
}

// writeHead writes the oldest segment. With wait it blocks until that segment is spilled.
func (p *BuildPipeline) writeHead(wait bool) error {
	j := p.jobs[0]
	if wait {
		<-j.done
	}
	p.observe(j)
	p.jobs = p.jobs[1:]
	defer j.spilled.Discard()
	if j.panicVal != nil {
		panic(j.panicVal)
	}
	if j.err != nil {
		return j.err
	}
	if err := p.cfg.Write(j.spilled); err != nil {
		return err
	}
	p.written++
	return nil
}

func (j *buildSegJob) isDone() bool {
	select {
	case <-j.done:
		return true
	default:
		return false
	}
}
