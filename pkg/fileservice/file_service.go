// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"context"
	"io"
)

// FileService is a write-once file system
type FileService interface {
	// Write writes a new file
	// returns ErrFileExisted if file already existed
	// returns ErrSizeNotMatch if provided size does not match data
	// entries in vector should be written atomically. if write failed, following reads must not succeed.
	Write(ctx context.Context, vector IOVector) error

	// Read reads a file to fill IOEntries
	// returns ErrFileNotFound if requested file not found
	// returns ErrUnexpectedEOF if less data is read than requested size
	// returns ErrEmptyRange if no data at specified offset and size
	Read(ctx context.Context, vector *IOVector) error

	// List lists sub-entries in a dir
	List(ctx context.Context, dirPath string) ([]DirEntry, error)

	// Delete deletes a file
	// returns ErrFileNotFound if requested file not found
	Delete(ctx context.Context, filePath string) error
}

type IOEntry struct {
	// offset in file, [0, len(file) - 1]
	Offset int

	// number of bytes to read or write, [1, len(file)]
	// when reading, pass -1 to read to the end of file
	Size int

	// raw content
	// when reading, if len(Data) < Size, a new Size-lengthed byte slice will be allocated
	Data []byte

	// when reading, if Writer is not nil, write data to it instead of setting Data field
	WriterForRead io.Writer

	// when reading, if ReadCloser is not nil, set an io.ReadCloser instead of setting Data field
	ReadCloserForRead *io.ReadCloser

	// when writing, if Reader is not nil, read data from it instead of reading Data field
	ReaderForWrite io.Reader
}

// DirEntry is a file or dir
type DirEntry struct {
	// file name, not full path
	Name  string
	IsDir bool
	Size  int
}
