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

package blockio

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type Meta struct {
	key        string
	loc        objectio.Extent
	rows       uint32
	objectSize uint32
}

func (m *Meta) GetKey() string          { return m.key }
func (m *Meta) GetLoc() objectio.Extent { return m.loc }
func (m *Meta) GetRows() uint32         { return m.rows }
func (m *Meta) GetObjectSize() uint32   { return m.objectSize }

type Delta struct {
	key        string
	loc        objectio.Extent
	objectSize uint32
}

func (d *Delta) GetKey() string          { return d.key }
func (d *Delta) GetLoc() objectio.Extent { return d.loc }
func (d *Delta) GetObjectSize() uint32   { return d.objectSize }

func DecodeMetaLocToMeta(metaLoc string) (*Meta, error) {
	info := strings.Split(metaLoc, ":")
	name := info[0]
	location := strings.Split(info[1], "_")
	offset, err := strconv.ParseUint(location[0], 10, 32)
	if err != nil {
		return nil, err
	}
	size, err := strconv.ParseUint(location[1], 10, 32)
	if err != nil {
		return nil, err
	}
	osize, err := strconv.ParseUint(location[2], 10, 32)
	if err != nil {
		return nil, err
	}
	rows, err := strconv.ParseUint(info[2], 10, 32)
	if err != nil {
		return nil, err
	}
	objectSize, err := strconv.ParseUint(info[3], 10, 32)
	if err != nil {
		return nil, err
	}
	extent := objectio.NewExtent(uint32(offset), uint32(size), uint32(osize))
	meta := &Meta{
		key:        name,
		loc:        extent,
		rows:       uint32(rows),
		objectSize: uint32(objectSize),
	}
	return meta, nil
}

func EncodeMetalocFromMetas(name string, blks []objectio.BlockObject) string {
	str := name
	for _, blk := range blks {
		str = fmt.Sprintf("%s:%d_%d_%d",
			str,
			blk.GetExtent().Offset(),
			blk.GetExtent().Length(),
			blk.GetExtent().OriginSize())
	}
	return str
}

func DecodeMetaLocToMetas(metaLoc string) (string, []objectio.Extent, error) {
	info := strings.Split(metaLoc, ":")
	name := info[0]
	extents := make([]objectio.Extent, 0)
	for i := 1; i < len(info); i++ {
		location := strings.Split(info[i], "_")
		offset, err := strconv.ParseUint(location[0], 10, 32)
		if err != nil {
			return "", nil, err
		}
		size, err := strconv.ParseUint(location[1], 10, 32)
		if err != nil {
			return "", nil, err
		}
		osize, err := strconv.ParseUint(location[2], 10, 32)
		if err != nil {
			return "", nil, err
		}
		extent := objectio.NewExtent(uint32(offset), uint32(size), uint32(osize))
		extents = append(extents, extent)
	}
	return name, extents, nil
}

func DecodeDeltaLocToDelta(metaLoc string) (*Delta, error) {
	info := strings.Split(metaLoc, ":")
	name := info[0]
	location := strings.Split(info[1], "_")
	offset, err := strconv.ParseUint(location[0], 10, 32)
	if err != nil {
		return nil, err
	}
	size, err := strconv.ParseUint(location[1], 10, 32)
	if err != nil {
		return nil, err
	}
	osize, err := strconv.ParseUint(location[2], 10, 32)
	if err != nil {
		return nil, err
	}
	objectSize, err := strconv.ParseUint(info[3], 10, 32)
	if err != nil {
		return nil, err
	}
	extent := objectio.NewExtent(uint32(offset), uint32(size), uint32(osize))
	delta := &Delta{
		key:        name,
		loc:        extent,
		objectSize: uint32(objectSize),
	}
	return delta, nil
}
