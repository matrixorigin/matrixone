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

package objectio

type ObjectMeta []byte

func (e ObjectMeta) MustGetMeta(metaType DataMetaType) ObjectDataMeta {
	return objectMetaV3(e[IOEntryHeaderSize:]).MustGetMeta(metaType)
}

func (e ObjectMeta) DataMetaCount() uint16 {
	return objectMetaV3(e[IOEntryHeaderSize:]).DataMetaCount()
}

func (e ObjectMeta) DataMeta() (ObjectDataMeta, bool) {
	return objectMetaV3(e[IOEntryHeaderSize:]).DataMeta()
}

func (e ObjectMeta) MustDataMeta() ObjectDataMeta {
	return objectMetaV3(e[IOEntryHeaderSize:]).MustDataMeta()
}

func (e ObjectMeta) MustTombstoneMeta() ObjectDataMeta {
	return objectMetaV3(e[IOEntryHeaderSize:]).MustTombstoneMeta()
}

func (e ObjectMeta) SubMeta(pos uint16) (ObjectDataMeta, bool) {
	return objectMetaV3(e[IOEntryHeaderSize:]).SubMeta(pos)
}

// SubMetaCount includes additive auxiliary sections. Object metadata has already
// been checked before accessing its optional V2/V3 section directory.
func (e ObjectMeta) SubMetaCount() uint16 {
	if len(e) < IOEntryHeaderSize || DecodeIOEntryHeader(e).Version < IOET_ObjectMeta_V2 {
		return 0
	}
	if len(e) < IOEntryHeaderSize+int(metaHeaderLen+schemaCountLen) {
		return 0
	}
	return objectMetaV3(e[IOEntryHeaderSize:]).SubMetaCount()
}
