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

package ioutil

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// with dirname
func EncodeCKPMetadataFullName(
	start, end types.TS,
) string {
	return fmt.Sprintf(
		"%s/%s_%s_%s.%s",
		GetCheckpointDir(),
		PrefixMetadata,
		start.ToString(),
		end.ToString(),
		CheckpointExt,
	)
}

// without dirname
func EncodeCKPMetadataName(
	start, end types.TS,
) string {
	return fmt.Sprintf(
		"%s_%s_%s.%s",
		PrefixMetadata,
		start.ToString(),
		end.ToString(),
		CheckpointExt,
	)
}

func DecodeCKPMetaName(name string) (meta TSRangeFile) {
	fileName := strings.Split(name, ".")
	info := strings.Split(fileName[0], "_")
	meta.start = types.StringToTS(info[1])
	meta.end = types.StringToTS(info[2])
	meta.ext = fileName[1]
	meta.name = name
	return
}

func EncodeCompactCKPMetadataFullName(
	start, end types.TS,
) string {
	return fmt.Sprintf(
		"%s/%s_%s_%s.%s",
		GetCheckpointDir(),
		PrefixMetadata,
		start.ToString(),
		end.ToString(),
		CompactExt,
	)
}

/*GC-Related*/

func MakeGCFullName(name string) string {
	return GetGCDir() + name
}

func EncodeGCMetadataName(start, end types.TS) string {
	return fmt.Sprintf(
		"%s_%s_%s.%s",
		PrefixGCMeta,
		start.ToString(),
		end.ToString(),
		GCMetaExt,
	)
}

func DecodeGCMetadataName(name string) (ret TSRangeFile) {
	fileName := strings.Split(name, ".")
	info := strings.Split(fileName[0], "_")
	ret.start = types.StringToTS(info[1])
	ret.end = types.StringToTS(info[2])
	ret.ext = fileName[1]
	ret.name = name
	return
}

func InheritGCMetadataName(name string, start, end *types.TS) string {
	fileName := strings.Split(name, ".")
	info := strings.Split(fileName[0], "_")
	prefix := info[0]
	ext := fileName[1]
	return fmt.Sprintf("%s_%s_%s.%s", prefix, start.ToString(), end.ToString(), ext)
}

func EncodeSnapshotMetadataName(start, end types.TS) string {
	return fmt.Sprintf(
		"%s_%s_%s.%s",
		PrefixSnapMeta,
		start.ToString(),
		end.ToString(),
		SnapshotExt,
	)
}

func EncodeAcctMetadataName(start, end types.TS) string {
	return fmt.Sprintf(
		"%s_%s_%s.%s",
		PrefixAcctMeta,
		start.ToString(),
		end.ToString(),
		AcctExt,
	)
}

/*Others*/

func EncodeTmpFileName(dir, prefix string, ts int64) string {
	return fmt.Sprintf("%s/%s_%d.%s", dir, prefix, ts, TmpExt)
}
