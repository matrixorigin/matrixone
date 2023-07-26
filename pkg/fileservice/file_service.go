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
	"hash"
	"io"
	"time"
)

// FileService is a write-once file system
type FileService interface {
	// Name is file service's name
	// service name is case-insensitive
	Name() string

	// Write writes a new file
	// returns ErrFileExisted if file already existed
	// returns ErrSizeNotMatch if provided size does not match data
	// entries in vector should be written atomically. if write failed, following reads must not succeed.
	Write(ctx context.Context, vector IOVector) error

	// Read reads a file to fill IOEntries
	// returns ErrFileNotFound if requested file not found
	// returns ErrUnexpectedEOF if less data is read than requested size
	// returns ErrEmptyRange if no data at specified offset and size
	// returns ErrEmptyVector if no IOEntry is passed
	Read(ctx context.Context, vector *IOVector) error

	// List lists sub-entries in a dir
	List(ctx context.Context, dirPath string) ([]DirEntry, error)

	// Delete deletes multi file
	// returns ErrFileNotFound if requested file not found
	Delete(ctx context.Context, filePaths ...string) error

	// Stat returns infomations about a file
	// returns ErrFileNotFound if requested file not found
	StatFile(ctx context.Context, filePath string) (*DirEntry, error)

	// Preload indicates the service to preload a file
	Preload(ctx context.Context, filePath string) error
}

type IOVector struct {
	// FilePath indicates where to find the file
	// a path has two parts, service name and file name, separated by ':'
	// service name is optional, if omitted, the receiver FileService will use the default name of the service
	// file name parts are separated by '/'
	// valid characters in file name: 0-9 a-z A-Z / ! - _ . * ' ( )
	// and all printable non-ASCII characters
	// example:
	// s3:a/b/c S3:a/b/c represents the same file 'a/b/c' located in 'S3' service
	FilePath string
	// io entries
	// empty Entries is not allowed
	// when writing, overlapping Entries is not allowed
	Entries []IOEntry
	// ExpireAt specifies the expire time of the file
	// implementations may or may not delete the file after this time
	// zero value means no expire
	ExpireAt time.Time
	// NoCache true, means the data NOT read/update FileService cache.
	NoCache bool
	// Preloading indicates whether the I/O is for preloading
	Preloading bool

	// Hash stores hash sum of written file if both Sum and New is not null
	// Hash.Sum may be incorrect if Write fails
	Hash Hash
}

type Hash struct {
	Sum *[]byte
	New func() hash.Hash
}

type IOEntry struct {
	// offset in file
	// when writing or mutating, offset can be arbitrary value, gaps between provided data are zero-filled
	// when reading, valid offsets are in range [0, len(file) - 1]
	Offset int64

	// number of bytes to read or write, [1, len(file)]
	// when reading, pass -1 to read to the end of file
	Size int64

	// raw content
	// when reading, if len(Data) < Size, a new Size-lengthed byte slice will be allocated
	Data []byte

	// when reading, if Writer is not nil, write data to it instead of setting Data field
	WriterForRead io.Writer

	// when reading, if ReadCloser is not nil, set an io.ReadCloser instead of setting Data field
	ReadCloserForRead *io.ReadCloser

	// when writing, if Reader is not nil, read data from it instead of reading Data field
	// number of bytes to be read is specified by Size field
	// if number of bytes is unknown, set Size field to -1
	ReaderForWrite io.Reader

	// When reading, if the ToObjectBytes field is not nil, the returning object's byte slice will be set to this field
	// Data, WriterForRead, ReadCloserForRead may be empty if ObjectBytes is not null
	// if ToObjectBytes is provided, caller should always read ObjectBytes instead of Data, WriterForRead or ReadCloserForRead
	ObjectBytes []byte

	// ToObjectBytes constructs an object byte slice from entry contents
	// reader or data must not be retained after returns
	// reader always contains entry contents
	// data may contains entry contents if available
	// if data is empty, the io.Reader must be fully read before returning nil error
	ToObjectBytes func(reader io.Reader, data []byte) (object []byte, objectSize int64, err error)

	// ObjectSize indicates the memory bytes to hold the object
	// set from ToObjectBytes returning value
	// used in capacity limited caches
	ObjectSize int64

	// done indicates whether the entry is filled with data
	// for implementing cascade cache
	done bool
}

// DirEntry is a file or dir
type DirEntry struct {
	// file name, not full path
	Name  string
	IsDir bool
	Size  int64
}
