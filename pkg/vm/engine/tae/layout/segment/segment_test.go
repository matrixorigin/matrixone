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

package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSegment_Init(t *testing.T) {
	seg := Segment{}
	seg.Init("1.seg")
	seg.Mount()
	file := seg.NewBlockFile("test")
	/*seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 513)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 514)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 515)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 516)))
	seg.Update(file, []byte(fmt.Sprintf("this is tests %d", 517)), 4096)
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 518)))*/
	for i := 0; i < 3; i++ {
		var sbuffer bytes.Buffer
		err := binary.Write(&sbuffer, binary.BigEndian, []byte(fmt.Sprintf("this is tests %d", 515)))
		assert.Nil(t, err)
		var size uint32 = 262144
		ibufLen := (size - (uint32(sbuffer.Len()) % size)) + uint32(sbuffer.Len())
		if ibufLen > uint32(sbuffer.Len()) {
			zero := make([]byte, ibufLen-uint32(sbuffer.Len()))
			err = binary.Write(&sbuffer, binary.BigEndian, zero)
			assert.Nil(t, err)
		}
		err = seg.Append(file, sbuffer.Bytes())
		assert.Nil(t, err)
		err = seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 514)))
		assert.Nil(t, err)
		err = seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 515)))
		assert.Nil(t, err)
		err = seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 516)))
		assert.Nil(t, err)
	}
	var sbuffer bytes.Buffer
	err := binary.Write(&sbuffer, binary.BigEndian, []byte(fmt.Sprintf("this is tests %d", 515)))
	assert.Nil(t, err)
	var size uint32 = 262144
	ibufLen := (size - (uint32(sbuffer.Len()) % size)) + uint32(sbuffer.Len())
	if ibufLen > uint32(sbuffer.Len()) {
		zero := make([]byte, ibufLen-uint32(sbuffer.Len()))
		err = binary.Write(&sbuffer, binary.BigEndian, zero)
		assert.Nil(t, err)
	}
	err = seg.Update(file, sbuffer.Bytes(), 16384)
	assert.Nil(t, err)
	b := bytes.NewBuffer(make([]byte, 1<<20))
	_, err = file.Read(0, uint32(file.snode.size), b.Bytes())
	assert.Nil(t, err)
	buf := b.Bytes()
	buf = buf[16384 : 16384+17]
	logutil.Infof("%v", string(buf))
	//seg.Update(file, []byte(fmt.Sprintf("this is tests %d", 517)), 8192)
	//seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 516)))
	//seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 516)))
	/*seg.Free(file, 1)
	seg.Free(file, 40)
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 513)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 514)))
	seg.Append(file, []byte(fmt.Sprintf("this is tests %d", 515)))*/
}
