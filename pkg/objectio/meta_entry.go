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
