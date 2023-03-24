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

import "io"

type readCloser struct {
	r         io.Reader
	closeFunc func() error
}

var _ io.ReadCloser = new(readCloser)

func (r *readCloser) Read(data []byte) (int, error) {
	return r.r.Read(data)
}

func (r *readCloser) Close() error {
	return r.closeFunc()
}

type countingReader struct {
	R io.Reader
	N int64
}

var _ io.Reader = new(countingReader)

func (c *countingReader) Read(data []byte) (int, error) {
	n, err := c.R.Read(data)
	c.N += int64(n)
	return n, err
}

type writeCloser struct {
	w         io.Writer
	closeFunc func() error
}

var _ io.WriteCloser = new(writeCloser)

func (r *writeCloser) Write(data []byte) (int, error) {
	return r.w.Write(data)
}

func (r *writeCloser) Close() error {
	return r.closeFunc()
}
