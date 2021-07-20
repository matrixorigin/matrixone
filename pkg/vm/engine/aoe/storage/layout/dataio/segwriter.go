package dataio

import (
	"matrixone/pkg/container/batch"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path/filepath"
)

type SegmentWriter struct {
	data         []*batch.Batch
	meta         *md.Segment
	dir          string
	fileHandle   *os.File
	preprocessor func([]*batch.Batch, *md.Segment) error
	fileGetter   func(string, *md.Segment) (*os.File, error)
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
	w.fileGetter = w.createFile
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

func (sw *SegmentWriter) createFile(dir string, meta *md.Segment) (*os.File, error) {
	id := meta.AsCommonID()
	filename := e.MakeSegmentFileName(dir, id.ToSegmentFileName(), meta.Table.ID)
	fdir := filepath.Dir(filename)
	if _, err := os.Stat(fdir); os.IsNotExist(err) {
		err = os.MkdirAll(fdir, 0755)
		if err != nil {
			return nil, err
		}
	}
	w, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	return w, err
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
	defer w.Close()
	if sw.preExecutor != nil {
		sw.preExecutor()
	}
	if err = sw.indexFlusher(w, sw.data, sw.meta); err != nil {
		return err
	}
	if err = sw.dataFlusher(w, sw.data, sw.meta); err != nil {
		return err
	}
	if sw.postExecutor != nil {
		sw.postExecutor()
	}
	return nil
}
