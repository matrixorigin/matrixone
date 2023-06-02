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
	"encoding/binary"

	"github.com/gogo/protobuf/proto"
)

// Encode encodes the RequestLabel and return bytes.
func (r *RequestLabel) Encode() ([]byte, error) {
	data, err := proto.Marshal(r)
	if err != nil {
		return nil, err
	}
	size := uint16(len(data))
	buf := new(bytes.Buffer)
	if err = binary.Write(buf, binary.LittleEndian, size); err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes the RequestLabel from bufio.Reader.
func (r *RequestLabel) Decode(reader *bufio.Reader) error {
	s, err := reader.Peek(2)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(s)
	var size uint16
	err = binary.Read(buf, binary.LittleEndian, &size)
	if err != nil {
		return err
	}
	data := make([]byte, int(size)+2)
	if reader.Buffered() < int(size)+2 {
		hr := 0
		for hr < int(size)+2 {
			l, err := reader.Read(data[hr:])
			if err != nil {
				return err
			}
			hr += l
		}
	} else {
		_, err = reader.Read(data)
		if err != nil {
			return err
		}
	}
	if err = proto.Unmarshal(data[2:], r); err != nil {
		return err
	}
	return nil
}
