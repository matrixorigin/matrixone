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
	"bytes"
	"io"
	"os"
)

func (e *IOEntry) setCachedData() error {
	if e.ToCacheData == nil {
		return nil
	}
	if len(e.Data) == 0 {
		return nil
	}
	bs, err := e.ToCacheData(bytes.NewReader(e.Data), e.Data)
	if err != nil {
		return err
	}
	e.CachedData = bs
	return nil
}

func (e *IOEntry) ReadFromOSFile(file *os.File) error {
	r := io.LimitReader(file, e.Size)

	if len(e.Data) < int(e.Size) {
		e.Data = make([]byte, e.Size)
	}

	n, err := io.ReadFull(r, e.Data)
	if err != nil {
		return err
	}
	if n != int(e.Size) {
		return io.ErrUnexpectedEOF
	}

	if e.WriterForRead != nil {
		if _, err := e.WriterForRead.Write(e.Data); err != nil {
			return err
		}
	}
	if e.ReadCloserForRead != nil {
		*e.ReadCloserForRead = io.NopCloser(bytes.NewReader(e.Data))
	}
	if err := e.setCachedData(); err != nil {
		return err
	}

	e.done = true

	return nil
}

func DataAsObject(r io.Reader, data []byte) (_ RCBytes, err error) {
	if len(data) > 0 {
		return RCBytesPool.GetAndCopy(data), nil
	}
	data, err = io.ReadAll(r)
	if err != nil {
		return
	}
	return RCBytesPool.GetAndCopy(data), nil
}
