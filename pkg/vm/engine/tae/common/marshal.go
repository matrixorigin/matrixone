// Copyright 2021 Matrix Origin
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

package common

import (
	"encoding/binary"
	"io"
)

func WriteString(str string, w io.Writer) (n int64, err error) {
	buf := []byte(str)
	if err = binary.Write(w, binary.BigEndian, uint16(len(buf))); err != nil {
		return
	}
	wn, err := w.Write(buf)
	return int64(wn + 2), err
}

func WriteBytes(b []byte, w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, uint16(len(b))); err != nil {
		return
	}
	wn, err := w.Write(b)
	return int64(wn + 2), err
}

func ReadString(r io.Reader) (str string, n int64, err error) {
	strLen := uint16(0)
	if err = binary.Read(r, binary.BigEndian, &strLen); err != nil {
		return
	}
	buf := make([]byte, strLen)
	if _, err = r.Read(buf); err != nil {
		return
	}
	str = string(buf)
	n = 2 + int64(strLen)
	return
}

func ReadBytes(r io.Reader) (buf []byte, n int64, err error) {
	strLen := uint16(0)
	if err = binary.Read(r, binary.BigEndian, &strLen); err != nil {
		return
	}
	buf = make([]byte, strLen)
	if _, err = r.Read(buf); err != nil {
		return
	}
	n = 2 + int64(strLen)
	return
}
