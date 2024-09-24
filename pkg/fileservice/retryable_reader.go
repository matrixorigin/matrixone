// Copyright 2023 Matrix Origin
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
	"io"
)

type retryableReader struct {
	current     io.ReadCloser
	offset      int64
	newReader   func(offset int64) (io.ReadCloser, error)
	shouldRetry func(error) bool
}

func newRetryableReader(
	newReader func(offset int64) (io.ReadCloser, error),
	initOffset int64,
	shouldRetry func(error) bool,
) (*retryableReader, error) {
	r, err := newReader(initOffset)
	if err != nil {
		return nil, err
	}
	return &retryableReader{
		current:     r,
		offset:      initOffset,
		newReader:   newReader,
		shouldRetry: shouldRetry,
	}, nil
}

var _ io.ReadCloser = new(retryableReader)

func (r *retryableReader) Read(buf []byte) (int, error) {
	for {
		n, err := r.current.Read(buf)
		if err != nil {
			if r.shouldRetry(err) {
				// retry
				newReader, err := r.newReader(r.offset)
				if err != nil {
					return 0, err
				}
				r.current.Close()
				r.current = newReader
				continue
			} else {
				// not retryable or EOF
				r.offset += int64(n)
				return n, err
			}
		}
		r.offset += int64(n)
		return n, nil
	}
}

func (r *retryableReader) Close() error {
	return r.current.Close()
}
