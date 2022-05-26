// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"bytes"
	"encoding/gob"
	"io"
)

func gobMarshalTo(w io.Writer, a any) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(a); err != nil {
		panic(err)
	}
	data := buf.Bytes()
	length := make([]byte, 8)
	binaryEnc.PutUint64(length, uint64(len(data)))
	if _, err := w.Write(length); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return nil
}

func gobUnmarshalFrom(r io.Reader, a any) error {
	length := make([]byte, 8)
	if _, err := io.ReadFull(r, length); err != nil {
		return err
	}
	data := make([]byte, binaryEnc.Uint64(length))
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(a)
}
