package dataio

import (
	"bytes"
	"encoding/binary"
	"io"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe/mergesort"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path/filepath"
	// log "github.com/sirupsen/logrus"
)

//  BlkCnt | Blk0 Pos | Blk1 Pos | ... | BlkEndPos | Blk0 Data | ...
type SegmentWriter struct {
	data         []*batch.Batch
	meta         *md.Segment
	dir          string
	fileHandle   *os.File
	preprocessor func([]*batch.Batch, *md.Segment) error
	fileGetter   func(string, *md.Segment) (*os.File, error)
	fileCommiter func(string) error
	indexFlusher func(*os.File, []*batch.Batch, *md.Segment) error
	dataFlusher  func(*os.File, []*batch.Batch, *md.Segment) error
	preExecutor  func()
	postExecutor func()
}

func NewSegmentWriter(data []*batch.Batch, meta *md.Segment, dir string) *SegmentWriter {
	w := &SegmentWriter{
		data: data,
		meta: meta,
		dir:  dir,
	}
	// w.preprocessor = w.defaultPreprocessor
	w.fileGetter, w.fileCommiter = w.createFile, w.commitFile
	w.dataFlusher = flushBlocks
	w.indexFlusher = w.flushIndices
	return w
}

func (sw *SegmentWriter) SetPreExecutor(f func()) {
	sw.preExecutor = f
}

func (sw *SegmentWriter) SetPostExecutor(f func()) {
	sw.postExecutor = f
}

func (sw *SegmentWriter) SetFileGetter(f func(string, *md.Segment) (*os.File, error)) {
	sw.fileGetter = f
}

func (sw *SegmentWriter) SetIndexFlusher(f func(*os.File, []*batch.Batch, *md.Segment) error) {
	sw.indexFlusher = f
}

func (sw *SegmentWriter) SetDataFlusher(f func(*os.File, []*batch.Batch, *md.Segment) error) {
	sw.dataFlusher = f
}

func (sw *SegmentWriter) defaultPreprocessor(data []*batch.Batch, meta *md.Segment) error {
	err := mergesort.MergeBlocksToSegment(data)
	return err
}

func (sw *SegmentWriter) commitFile(fname string) error {
	name, err := e.FilenameFromTmpfile(fname)
	if err != nil {
		return err
	}
	err = os.Rename(fname, name)
	return err
}

func (sw *SegmentWriter) createFile(dir string, meta *md.Segment) (*os.File, error) {
	id := meta.AsCommonID()
	filename := e.MakeSegmentFileName(dir, id.ToSegmentFileName(), meta.Table.ID, true)
	fdir := filepath.Dir(filename)
	if _, err := os.Stat(fdir); os.IsNotExist(err) {
		err = os.MkdirAll(fdir, 0755)
		if err != nil {
			return nil, err
		}
	}
	w, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	return w, err
}

func (sw *SegmentWriter) flushIndices(w *os.File, data []*batch.Batch, meta *md.Segment) error {
	return nil
}

func (sw *SegmentWriter) Execute() error {
	if sw.preprocessor != nil {
		if err := sw.preprocessor(sw.data, sw.meta); err != nil {
			return err
		}
	}
	w, err := sw.fileGetter(sw.dir, sw.meta)
	if err != nil {
		return err
	}
	sw.fileHandle = w
	if sw.preExecutor != nil {
		sw.preExecutor()
	}
	if err = sw.indexFlusher(w, sw.data, sw.meta); err != nil {
		w.Close()
		return err
	}
	if err = sw.dataFlusher(w, sw.data, sw.meta); err != nil {
		w.Close()
		return err
	}
	if sw.postExecutor != nil {
		sw.postExecutor()
	}
	filename, _ := filepath.Abs(w.Name())
	w.Close()
	return sw.fileCommiter(filename)
}

func flushBlocks(w *os.File, data []*batch.Batch, meta *md.Segment) error {
	// Write Header
	// Write Indice
	// Write Blocks
	err := binary.Write(w, binary.BigEndian, uint32(len(data)))
	if err != nil {
		return err
	}
	for _, blk := range meta.Blocks {
		if err = binary.Write(w, binary.BigEndian, blk.ID); err != nil {
			return err
		}
	}
	startPos, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if _, err = w.Seek(int64(4*(len(data)+1)), io.SeekCurrent); err != nil {
		return err
	}

	blkMetaPos := make([]uint32, len(data))
	for i, bat := range data {
		pos, _ := w.Seek(0, io.SeekCurrent)
		blkMetaPos[i] = uint32(pos)
		getter := func(string, *md.Block) (*os.File, error) {
			return w, nil
		}
		writer := NewEmbbedBlockWriter(bat, meta.Blocks[i], getter)
		if err = writer.Execute(); err != nil {
			return err
		}
	}
	blkEndPos, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	for _, pos := range blkMetaPos {
		binary.Write(&buf, binary.BigEndian, pos)
	}
	binary.Write(&buf, binary.BigEndian, uint32(blkEndPos))
	if _, err = w.WriteAt(buf.Bytes(), startPos); err != nil {
		return err
	}
	if _, err = w.Seek(blkEndPos, io.SeekStart); err != nil {
		return err
	}
	return nil
}
