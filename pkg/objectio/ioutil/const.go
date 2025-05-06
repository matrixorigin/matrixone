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

const (
	TableObjectsAttr_Accout_Idx     = 0
	TableObjectsAttr_DB_Idx         = 1
	TableObjectsAttr_Table_Idx      = 2
	TableObjectsAttr_ObjectType_Idx = 3
	TableObjectsAttr_ID_Idx         = 4
	TableObjectsAttr_CreateTS_Idx   = 5
	TableObjectsAttr_DeleteTS_Idx   = 6
	TableObjectsAttr_Cluster_Idx    = 7
	// TableObjectsAttr_PhysicalAddr_Idx = 8
)

const (
	ObjectType_Invalid int8 = iota
	ObjectType_Data
	ObjectType_Tombstone
)
