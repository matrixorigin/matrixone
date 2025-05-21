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

// it is very special for history reason <@_@>!
// here the dir should be like : ckp/ or gc/ or debug/
func MakeFullName(dir, name string) string {
	return dir + name
}

func IsFullName(name string) bool {
	return strings.Contains(name, "/")
}

// name is expected to be like `meta_<start_ts>_<end_ts>.<ext>`
func DecodeTSRangeFile(name string) (meta TSRangeFile) {
	fileName := strings.Split(name, ".")
	// fileName is expected to be like []string{"meta_<start_ts>_<end_ts>", "<ext>"}
	if len(fileName) != 2 {
		meta.ext = InvalidExt
		return
	}
	info := strings.Split(fileName[0], "_")
	// info is expected to be like []string{"meta", "<start_ts>", "<end_ts>"}
	if len(info) != 3 {
		meta.ext = InvalidExt
		return
	}
	meta.start = types.StringToTS(info[1])
	meta.end = types.StringToTS(info[2])
	meta.name = name
	meta.ext = fileName[1]
	return
}

// name may contain dir name
// name is expected to be a encoded from TSRangeFile
func TryDecodeTSRangeFile(name string) (dir string, ret TSRangeFile) {
	fname := name
	if IsFullName(name) {
		if strings.HasPrefix(name, GetCheckpointDir()) {
			names := strings.Split(name, GetCheckpointDir()+"/")
			if len(names) != 2 {
				ret.SetInvalid()
				return
			}
			dir = GetCheckpointDir()
			fname = names[1]
		} else if strings.HasPrefix(name, GetGCDir()) {
			names := strings.Split(name, GetGCDir())
			if len(names) != 2 {
				ret.SetInvalid()
				return
			}
			dir = GetGCDir()
			fname = names[1]
		} else {
			ret.SetInvalid()
			return
		}
	}
	ret = DecodeTSRangeFile(fname)
	return
}

// with dirname
// input: start=0, end=100
// output: /ckp/meta_0_100.ckp
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
	return DecodeTSRangeFile(name)
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
	return MakeFullName(GetGCDir(), name)
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
	return DecodeTSRangeFile(name)
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
