package engine

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine"
	"unsafe"
)

func (a aoeReader) NewFilter() engine.Filter {
	return nil
}

func (a aoeReader) NewSummarizer() engine.Summarizer {
	return nil
}

func (a aoeReader) NewSparseFilter() engine.SparseFilter {
	return nil
}

func (a aoeReader) Read(refCount []uint64, attrs []string) (*batch.Batch, error) {
	if a.blocks == nil || len(*(a.blocks)) == 0 {
		return nil, nil
	}
	compressedBytes := make([]*bytes.Buffer, len(refCount))
	decompressedBytes := make([]*bytes.Buffer, len(refCount))

	for i := range refCount {
		compressedBytes[i] = bytes.NewBuffer(make([]byte, 0, 8))
		decompressedBytes[i] = bytes.NewBuffer(make([]byte, 0, 8))
	}
	(*(a.blocks))[0].Prefetch(attrs)

	bat, err := (*(a.blocks))[0].Read(refCount, attrs, compressedBytes, decompressedBytes)
	if err != nil {
		return nil, err
	}
	logutil.Infof("vecs len is %d", vector.Length(bat.Vecs[0]))

	//zs, err := mheap.Alloc(a.mp, int64(vector.Length(bat.Vecs[0])))
	zs := make([]*bytes.Buffer, int64(vector.Length(bat.Vecs[0])))
	if err != nil {
		return nil, err
	}

	bat.Zs = unsafe.Slice((*int64)(unsafe.Pointer(&zs[0])), len(zs)/8)
	for i := 0; i < len(bat.Zs); i++ {
		bat.Zs[i] = 1
	}
	if len(*(a.blocks)) > 1 {
		*(a.blocks) = append((*(a.blocks))[:1], (*(a.blocks))[2:]...)
	} else if len(*(a.blocks)) == 1 {
		*(a.blocks) = (*(a.blocks))[0:0]
	}
	logutil.Infof("bat is %v", bat)
	return bat, nil
}
