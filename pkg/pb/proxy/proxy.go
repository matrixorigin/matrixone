// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"bufio"
	"bytes"
	io "io"
)

func (i *ExtraInfo) Encode() ([]byte, error) {
	data, err := i.Marshal()
	if err != nil {
		return nil, err
	}
	size := len(data)
	ret := make([]byte, 2, len(data)+2)
	ret[0] = uint8(size)
	ret[1] = uint8(size >> 8)
	ret = append(ret, data...)
	return ret, nil
}

func (i *ExtraInfo) Decode(reader *bufio.Reader) error {
	data, err := readData(reader)
	if err != nil {
		return err
	}
	return i.Unmarshal(data[2:])
}

// readData reads all data in the reader.
func readData(reader *bufio.Reader) ([]byte, error) {
	s, err := reader.Peek(2)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(s)
	size := uint16(buf.Bytes()[0]) + uint16(buf.Bytes()[1])<<8 + 2
	data := make([]byte, size)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}
