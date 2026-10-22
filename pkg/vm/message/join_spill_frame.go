// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package message

import (
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const JoinSpillMagic = uint64(0x12345678DEADBEEF)

type spillCountingWriter struct {
	writer io.Writer
	bytes  int64
}

func (w *spillCountingWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.bytes += int64(n)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

// WriteJoinSpillFrame writes the persistent batch format directly to an anonymous
// file. The frame length is patched only after a complete encode and trailer.
// The caller owns the file and discards it on any returned error.
func WriteJoinSpillFrame(file *os.File, bat *batch.Batch) (int64, error) {
	offset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	writer := &spillCountingWriter{writer: file}
	count, size, magic := int64(bat.RowCount()), int64(0), JoinSpillMagic
	if _, err = writer.Write(types.EncodeInt64(&count)); err != nil {
		return writer.bytes, err
	}
	if _, err = writer.Write(types.EncodeInt64(&size)); err != nil {
		return writer.bytes, err
	}
	if err = bat.MarshalBinaryTo(writer); err != nil {
		return writer.bytes, err
	}
	size = writer.bytes - 16
	if _, err = writer.Write(types.EncodeUint64(&magic)); err != nil {
		return writer.bytes, err
	}
	n, err := file.WriteAt(types.EncodeInt64(&size), offset+8)
	if err == nil && n != 8 {
		err = io.ErrShortWrite
	}
	return writer.bytes, err
}
