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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ExtraInfo is the information that proxy send to CN when build
// connection in handshake phase.
type ExtraInfo struct {
	// Salt is the bytes in MySQL protocol which is 20 length.
	Salt []byte
	// InternalConn indicates whether the connection is from internal
	// network or external network. false means that it is external, otherwise,
	// it is internal.
	InternalConn bool
	// Label is the requested label from client.
	Label RequestLabel
}

// Encode encodes the ExtraInfo and return bytes.
func (e *ExtraInfo) Encode() ([]byte, error) {
	if len(e.Salt) != 20 {
		return nil, moerr.NewInternalErrorNoCtx("invalid slat data, length is %d", len(e.Salt))
	}
	var err error
	var labelData []byte
	labelData, err = e.Label.Marshal()
	if err != nil {
		return nil, err
	}
	size := uint16(len(labelData)) + 20 + 1
	buf := new(bytes.Buffer)
	if err = binary.Write(buf, binary.LittleEndian, size); err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, e.Salt)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, e.InternalConn)
	if err != nil {
		return nil, err
	}
	if len(labelData) > 0 {
		err = binary.Write(buf, binary.LittleEndian, labelData)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// Decode decodes the ExtraInfo from bufio.Reader.
func (e *ExtraInfo) Decode(reader *bufio.Reader) error {
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
	if size < 21 {
		return moerr.NewInternalErrorNoCtx("invalid data length %d", size)
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
	buf = bytes.NewBuffer(data[2:])
	e.Salt = make([]byte, 20)
	if err = binary.Read(buf, binary.LittleEndian, e.Salt); err != nil {
		return err
	}
	if err = binary.Read(buf, binary.LittleEndian, &e.InternalConn); err != nil {
		return err
	}
	if size > 21 {
		labelData := make([]byte, size-21)
		if err = binary.Read(buf, binary.LittleEndian, labelData); err != nil {
			return err
		}
		if err = e.Label.Unmarshal(labelData); err != nil {
			return err
		}
	}
	return nil
}
