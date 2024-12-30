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

import "strings"

const defaultCheckpointDir = "ckp/"
const defaultGCDir = "gc/"

const (
	// checkpint related
	CheckpointExt = "ckp"
	CompactExt    = "cpt"

	// gc related
	GCFullExt   = "fgc"
	SnapshotExt = "snap"
	AcctExt     = "acct"
	TmpExt      = "tmp"
	GCMetaExt   = CheckpointExt

	// invalid ext
	InvalidExt = "%$%#"

	PrefixMetadata = "meta"
	SuffixMetadata = ".ckp"

	PrefixGCMeta   = "gc"
	PrefixSnapMeta = "snap"
	PrefixAcctMeta = "acct"
)

func GetCheckpointDir() string {
	return defaultCheckpointDir
}

func GetGCDir() string {
	return defaultGCDir
}

func IsCKPExt(ext string) bool {
	return ext == CheckpointExt
}

func IsGCMetaExt(ext string) bool {
	return ext == GCMetaExt
}

func IsMetadataName(name string) bool {
	return strings.HasPrefix(name, PrefixMetadata) && strings.HasSuffix(name, SuffixMetadata)
}
