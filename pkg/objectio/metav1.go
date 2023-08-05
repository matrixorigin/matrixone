// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

type objectMetaV1 []byte

func buildObjectMetaV1(count uint16) objectMetaV1 {
	length := headerLen + uint32(count)*colMetaLen
	buf := make([]byte, length)
	return buf[:]
}

func (o objectMetaV1) MustGetMeta(metaType DataMetaType) ObjectDataMeta {
	//TODO implement me
	panic("implement me")
}

func (o objectMetaV1) HeaderLength() uint32 {
	//TODO implement me
	panic("implement me")
}

func (o objectMetaV1) DataMetaCount() uint16 {
	//TODO implement me
	panic("implement me")
}

func (o objectMetaV1) TombstoneMetaCount() uint16 {
	//TODO implement me
	panic("implement me")
}

func (o objectMetaV1) DataMeta() (ObjectDataMeta, bool) {
	return ObjectDataMeta(o), true
}

func (o objectMetaV1) MustDataMeta() ObjectDataMeta {
	return ObjectDataMeta(o)
}

func (o objectMetaV1) TombstoneMeta() (ObjectDataMeta, bool) {
	return ObjectDataMeta(o), true
}

func (o objectMetaV1) MustTombstoneMeta() ObjectDataMeta {
	return ObjectDataMeta(o)
}

func (o objectMetaV1) SetDataMetaCount(count uint16) {
	//TODO implement me
	panic("implement me")
}

func (o objectMetaV1) SetDataMetaOffset(offset uint32) {
	//TODO implement me
	panic("implement me")
}

func (o objectMetaV1) SetTombstoneMetaCount(count uint16) {
	//TODO implement me
	panic("implement me")
}

func (o objectMetaV1) SetTombstoneMetaOffset(offset uint32) {
	//TODO implement me
	panic("implement me")
}

func (o objectMetaV1) SubMeta(pos uint16) (ObjectDataMeta, bool) {
	//TODO implement me
	panic("implement me")
}

func (o objectMetaV1) SubMetaCount() uint16 {
	//TODO implement me
	panic("implement me")
}

func (o objectMetaV1) SubMetaIndex() SubMetaIndex {
	//TODO implement me
	panic("implement me")
}

func (o objectMetaV1) SubMetaTypes() []uint16 {
	//TODO implement me
	panic("implement me")
}
