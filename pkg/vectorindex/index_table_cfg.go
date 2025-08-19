// Copyright 2022 Matrix Origin
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

package vectorindex

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type IndexTableCfg = IndexTableCfgV1

type IndexTableCfgV1 []byte

const (
	IndexTableCfg_MagicNumber = 0xa9a9
)

const (
	IndexTableCfg_ExtraCfgType_Empty = iota
	IndexTableCfg_ExtraCfgType_IVFFLAT
)

var magicNumberBuf = types.EncodeFixed(uint16(IndexTableCfg_MagicNumber))

// V1 Layout
// MagicNumber: uint16 [2 Bytes]
// DBNameOffset: uint32 [4 Bytes]
// DBNameLen: uint16 [2 Bytes]
// SrcTableOffset: uint32 [4 Bytes]
// SrcTableLen: uint16 [2 Bytes]
// MetadataTableOffset: uint32 [4 Bytes]
// MetadataTableLen: uint16 [2 Bytes]
// IndexTableOffset: uint32 [4 Bytes]
// IndexTableLen: uint16 [2 Bytes]
// PKeyOffset: uint32 [4 Bytes]
// PKeyLen: uint16 [2 Bytes]
// KeyPartOffset: uint32 [4 Bytes]
// KeyPartLen: uint16 [2 Bytes]
// ThreadsBuild: int64 [8 Bytes]
// ThreadsSearch: int64 [8 Bytes]
// IndexCapacity: int64 [8 Bytes]
// ExtraCfgType: uint16 [2 Bytes]
// ExtraCfgOffset: uint32 [4 Bytes]

const (
	IndexTableCfg_V1_MagicNumberOff        = 0
	IndexTableCfg_V1_DBNameStartOff        = IndexTableCfg_V1_MagicNumberOff + 2
	IndexTableCfg_V1_DBNameStartLen        = 4
	IndexTableCfg_V1_DBNameLenOff          = IndexTableCfg_V1_DBNameStartOff + IndexTableCfg_V1_DBNameStartLen
	IndexTableCfg_V1_DBNameLenLen          = 2
	IndexTableCfg_V1_SrcTableStartOff      = IndexTableCfg_V1_DBNameLenOff + IndexTableCfg_V1_DBNameLenLen
	IndexTableCfg_V1_SrcTableStartLen      = 4
	IndexTableCfg_V1_SrcTableLenOff        = IndexTableCfg_V1_SrcTableStartOff + IndexTableCfg_V1_SrcTableStartLen
	IndexTableCfg_V1_SrcTableLenLen        = 2
	IndexTableCfg_V1_MetadataTableStartOff = IndexTableCfg_V1_SrcTableLenOff + IndexTableCfg_V1_SrcTableLenLen
	IndexTableCfg_V1_MetadataTableStartLen = 4
	IndexTableCfg_V1_MetadataTableLenOff   = IndexTableCfg_V1_MetadataTableStartOff + IndexTableCfg_V1_MetadataTableStartLen
	IndexTableCfg_V1_MetadataTableLenLen   = 2
	IndexTableCfg_V1_IndexTableStartOff    = IndexTableCfg_V1_MetadataTableLenOff + IndexTableCfg_V1_MetadataTableLenLen
	IndexTableCfg_V1_IndexTableStartLen    = 4
	IndexTableCfg_V1_IndexTableLenOff      = IndexTableCfg_V1_IndexTableStartOff + IndexTableCfg_V1_IndexTableStartLen
	IndexTableCfg_V1_IndexTableLenLen      = 2
	IndexTableCfg_V1_PKeyStartOff          = IndexTableCfg_V1_IndexTableLenOff + IndexTableCfg_V1_IndexTableLenLen
	IndexTableCfg_V1_PKeyStartLen          = 4
	IndexTableCfg_V1_PKeyLenOff            = IndexTableCfg_V1_PKeyStartOff + IndexTableCfg_V1_PKeyStartLen
	IndexTableCfg_V1_PKeyLenLen            = 2
	IndexTableCfg_V1_KeyPartStartOff       = IndexTableCfg_V1_PKeyLenOff + IndexTableCfg_V1_PKeyLenLen
	IndexTableCfg_V1_KeyPartStartLen       = 4
	IndexTableCfg_V1_KeyPartLenOff         = IndexTableCfg_V1_KeyPartStartOff + IndexTableCfg_V1_KeyPartStartLen
	IndexTableCfg_V1_KeyPartLenLen         = 2
	IndexTableCfg_V1_ThreadsBuildOff       = IndexTableCfg_V1_KeyPartLenOff + IndexTableCfg_V1_KeyPartLenLen
	IndexTableCfg_V1_ThreadsBuildLen       = 8
	IndexTableCfg_V1_ThreadsSearchOff      = IndexTableCfg_V1_ThreadsBuildOff + IndexTableCfg_V1_ThreadsBuildLen
	IndexTableCfg_V1_ThreadsSearchLen      = 8
	IndexTableCfg_V1_IndexCapacityOff      = IndexTableCfg_V1_ThreadsSearchOff + IndexTableCfg_V1_ThreadsSearchLen
	IndexTableCfg_V1_IndexCapacityLen      = 8
	IndexTableCfg_V1_ExtraCfgTypeOff       = IndexTableCfg_V1_IndexCapacityOff + IndexTableCfg_V1_IndexCapacityLen
	IndexTableCfg_V1_ExtraCfgTypeLen       = 2
	IndexTableCfg_V1_ExtraCfgStartOff      = IndexTableCfg_V1_ExtraCfgTypeOff + IndexTableCfg_V1_ExtraCfgTypeLen
	IndexTableCfg_V1_ExtraCfgStartLen      = 4
	IndexTableCfg_V1_ExtraCfgLenOff        = IndexTableCfg_V1_ExtraCfgStartOff + IndexTableCfg_V1_ExtraCfgStartLen
	IndexTableCfg_V1_ExtraCfgLenLen        = 2
	IndexTableCfg_V1_HeaderLen             = IndexTableCfg_V1_ExtraCfgStartOff + IndexTableCfg_V1_ExtraCfgStartLen
)

// V1 ExtraIVFCfg_V1  Layout
// DataSize: uint64 [8 Bytes]
// Nprobe: uint32 [4 Bytes]
// PKeyType: int32 [4 Bytes]
// KeyPartType: int32 [4 Bytes]
// KmeansTrainPercent: int64 [8 Bytes]
// KmeansMaxIteration: int64 [8 Bytes]
// EntriesTableLen: uint16 [2 Bytes]
// EntriesTableData

const (
	ExtraIVFCfg_V1_DataSizeOff           = 0
	ExtraIVFCfg_V1_DataSizeLen           = 8
	ExtraIVFCfg_V1_NprobeOff             = ExtraIVFCfg_V1_DataSizeOff + ExtraIVFCfg_V1_DataSizeLen
	ExtraIVFCfg_V1_NprobeLen             = 4
	ExtraIVFCfg_V1_PKeyTypeOff           = ExtraIVFCfg_V1_NprobeOff + ExtraIVFCfg_V1_NprobeLen
	ExtraIVFCfg_V1_PKeyTypeLen           = 4
	ExtraIVFCfg_V1_KeyPartTypeOff        = ExtraIVFCfg_V1_PKeyTypeOff + ExtraIVFCfg_V1_PKeyTypeLen
	ExtraIVFCfg_V1_KeyPartTypeLen        = 4
	ExtraIVFCfg_V1_KmeansTrainPercentOff = ExtraIVFCfg_V1_KeyPartTypeOff + ExtraIVFCfg_V1_KeyPartTypeLen
	ExtraIVFCfg_V1_KmeansTrainPercentLen = 8
	ExtraIVFCfg_V1_KmeansMaxIterationOff = ExtraIVFCfg_V1_KmeansTrainPercentOff + ExtraIVFCfg_V1_KmeansTrainPercentLen
	ExtraIVFCfg_V1_KmeansMaxIterationLen = 8
	ExtraIVFCfg_V1_EntriesTableLenOff    = ExtraIVFCfg_V1_KmeansMaxIterationOff + ExtraIVFCfg_V1_KmeansMaxIterationLen
	ExtraIVFCfg_V1_EntriesTableLenLen    = 2
	ExtraIVFCfg_V1_EntriesTableHeaderLen = ExtraIVFCfg_V1_EntriesTableLenOff + ExtraIVFCfg_V1_EntriesTableLenLen
)

func CalculateIndexTableCfgV1Size(
	dbNameLen int,
	srcTableLen int,
	metadataTableLen int,
	indexTableLen int,
	pKeyLen int,
	keyPartLen int,
) int {
	return IndexTableCfg_V1_HeaderLen +
		dbNameLen +
		srcTableLen +
		metadataTableLen +
		indexTableLen +
		pKeyLen +
		keyPartLen
}

func CalculateExtraIVFCfgV1Size(
	entriesTableLen int,
) int {
	return ExtraIVFCfg_V1_EntriesTableHeaderLen + entriesTableLen
}

func BuildIVFIndexTableCfgV1(
	dbName string,
	srcTable string,
	metadataTable string,
	indexTable string,
	pKey string,
	keyPart string,
	threadsSearch int64,
	threadsBuild int64,
	indexCapacity int64,
	entriesTable string, // ivf related
	dataSize int64, // ivf related
	nprobe uint32, // ivf related
	pKeyType int32, // ivf related
	keyPartType int32, // ivf related
	kmeansTrainPercent int64, // ivf related
	kmeansMaxIteration int64, // ivf related
) IndexTableCfg {
	size1 := CalculateIndexTableCfgV1Size(
		len(dbName),
		len(srcTable),
		len(metadataTable),
		len(indexTable),
		len(pKey),
		len(keyPart),
	)
	size2 := CalculateExtraIVFCfgV1Size(len(entriesTable))
	size := size1 + size2
	cfg := MakeEmptyIndexTableCfgV1(size, IndexTableCfg_ExtraCfgType_IVFFLAT)
	cfg.SetStringValues(
		dbName,
		srcTable,
		metadataTable,
		indexTable,
		pKey,
		keyPart,
	)
	cfg.SetInt64Values(
		threadsSearch,
		threadsBuild,
		indexCapacity,
	)
	// Set the extra config start offset
	copy(cfg[IndexTableCfg_V1_ExtraCfgStartOff:], types.EncodeFixed(uint32(size1)))

	ivfCfg := ExtraIVFCfgV1(cfg[size1:])
	ivfCfg.SetFixedValues(
		dataSize,
		nprobe,
		pKeyType,
		keyPartType,
		kmeansTrainPercent,
		kmeansMaxIteration,
	)
	ivfCfg.SetEntriesTable(entriesTable)
	return cfg
}

func MakeEmptyIndexTableCfgV1(
	size int,
	extraCfgType int16,
) IndexTableCfgV1 {
	cfg := IndexTableCfgV1(make([]byte, size))
	copy(cfg, magicNumberBuf)
	cfg.SetExtraCfgType(extraCfgType)
	return cfg
}

func BuildIndexTableCfgV1(
	dbName string,
	srcTable string,
	metadataTable string,
	indexTable string,
	pKey string,
	keyPart string,
	threadsSearch int64,
	threadsBuild int64,
	indexCapacity int64,
) IndexTableCfgV1 {
	size := CalculateIndexTableCfgV1Size(
		len(dbName),
		len(srcTable),
		len(metadataTable),
		len(indexTable),
		len(pKey),
		len(keyPart),
	)
	cfg := MakeEmptyIndexTableCfgV1(size, IndexTableCfg_ExtraCfgType_Empty)
	cfg.SetStringValues(
		dbName,
		srcTable,
		metadataTable,
		indexTable,
		pKey,
		keyPart,
	)
	cfg.SetInt64Values(
		threadsSearch,
		threadsBuild,
		indexCapacity,
	)
	return cfg
}

func (cfg IndexTableCfgV1) IsCfgV1() bool {
	return types.DecodeFixed[uint16](cfg[IndexTableCfg_V1_MagicNumberOff:]) == IndexTableCfg_MagicNumber
}

func (cfg IndexTableCfgV1) SetExtraCfgType(
	extraCfgType int16,
) IndexTableCfgV1 {
	copy(cfg[IndexTableCfg_V1_ExtraCfgTypeOff:], types.EncodeFixed(extraCfgType))
	return cfg
}

func (cfg IndexTableCfgV1) SetStringValues(
	dbName string,
	srcTable string,
	metadataTable string,
	indexTable string,
	pKey string,
	keyPart string,
) IndexTableCfgV1 {
	dbNameLen := len(dbName)
	srcTableLen := len(srcTable)
	metadataTableLen := len(metadataTable)
	indexTableLen := len(indexTable)
	pKeyLen := len(pKey)
	keyPartLen := len(keyPart)

	copy(cfg[IndexTableCfg_V1_DBNameLenOff:], types.EncodeFixed(uint16(dbNameLen)))
	copy(cfg[IndexTableCfg_V1_SrcTableLenOff:], types.EncodeFixed(uint16(srcTableLen)))
	copy(cfg[IndexTableCfg_V1_MetadataTableLenOff:], types.EncodeFixed(uint16(metadataTableLen)))
	copy(cfg[IndexTableCfg_V1_IndexTableLenOff:], types.EncodeFixed(uint16(indexTableLen)))
	copy(cfg[IndexTableCfg_V1_PKeyLenOff:], types.EncodeFixed(uint16(pKeyLen)))
	copy(cfg[IndexTableCfg_V1_KeyPartLenOff:], types.EncodeFixed(uint16(keyPartLen)))

	// Calculate offsets relative to the start of the data section (after header)
	dataStart := IndexTableCfg_V1_HeaderLen
	dbNameStartOff := dataStart
	srcTableStartOff := dbNameStartOff + dbNameLen
	metadataTableStartOff := srcTableStartOff + srcTableLen
	indexTableStartOff := metadataTableStartOff + metadataTableLen
	pKeyStartOff := indexTableStartOff + indexTableLen
	keyPartStartOff := pKeyStartOff + pKeyLen

	// Copy string data to the data section
	copy(cfg[dbNameStartOff:], dbName)
	copy(cfg[srcTableStartOff:], srcTable)
	copy(cfg[metadataTableStartOff:], metadataTable)
	copy(cfg[indexTableStartOff:], indexTable)
	copy(cfg[pKeyStartOff:], pKey)
	copy(cfg[keyPartStartOff:], keyPart)

	// Store the offsets in the header
	copy(cfg[IndexTableCfg_V1_DBNameStartOff:], types.EncodeFixed(uint32(dbNameStartOff)))
	copy(cfg[IndexTableCfg_V1_SrcTableStartOff:], types.EncodeFixed(uint32(srcTableStartOff)))
	copy(cfg[IndexTableCfg_V1_MetadataTableStartOff:], types.EncodeFixed(uint32(metadataTableStartOff)))
	copy(cfg[IndexTableCfg_V1_IndexTableStartOff:], types.EncodeFixed(uint32(indexTableStartOff)))
	copy(cfg[IndexTableCfg_V1_PKeyStartOff:], types.EncodeFixed(uint32(pKeyStartOff)))
	copy(cfg[IndexTableCfg_V1_KeyPartStartOff:], types.EncodeFixed(uint32(keyPartStartOff)))

	return cfg
}

func (cfg IndexTableCfgV1) SetInt64Values(
	threadsSearch int64,
	threadsBuild int64,
	indexCapacity int64,
) IndexTableCfgV1 {
	copy(cfg[IndexTableCfg_V1_ThreadsSearchOff:], types.EncodeFixed(threadsSearch))
	copy(cfg[IndexTableCfg_V1_ThreadsBuildOff:], types.EncodeFixed(threadsBuild))
	copy(cfg[IndexTableCfg_V1_IndexCapacityOff:], types.EncodeFixed(indexCapacity))
	return cfg
}

func (cfg IndexTableCfgV1) DBName() string {
	dbNameLen := types.DecodeFixed[uint16](cfg[IndexTableCfg_V1_DBNameLenOff:])
	dbNameStartOff := types.DecodeFixed[uint32](cfg[IndexTableCfg_V1_DBNameStartOff:])
	dbName := string(cfg[dbNameStartOff : dbNameStartOff+uint32(dbNameLen)])
	return dbName
}

func (cfg IndexTableCfgV1) SrcTable() string {
	srcTableLen := types.DecodeFixed[uint16](cfg[IndexTableCfg_V1_SrcTableLenOff:])
	srcTableStartOff := types.DecodeFixed[uint32](cfg[IndexTableCfg_V1_SrcTableStartOff:])
	srcTable := string(cfg[srcTableStartOff : srcTableStartOff+uint32(srcTableLen)])
	return srcTable
}

func (cfg IndexTableCfgV1) MetadataTable() string {
	metadataTableLen := types.DecodeFixed[uint16](cfg[IndexTableCfg_V1_MetadataTableLenOff:])
	metadataTableStartOff := types.DecodeFixed[uint32](cfg[IndexTableCfg_V1_MetadataTableStartOff:])
	metadataTable := string(cfg[metadataTableStartOff : metadataTableStartOff+uint32(metadataTableLen)])
	return metadataTable
}

func (cfg IndexTableCfgV1) IndexTable() string {
	indexTableLen := types.DecodeFixed[uint16](cfg[IndexTableCfg_V1_IndexTableLenOff:])
	indexTableStartOff := types.DecodeFixed[uint32](cfg[IndexTableCfg_V1_IndexTableStartOff:])
	indexTable := string(cfg[indexTableStartOff : indexTableStartOff+uint32(indexTableLen)])
	return indexTable
}

func (cfg IndexTableCfgV1) PKey() string {
	pKeyLen := types.DecodeFixed[uint16](cfg[IndexTableCfg_V1_PKeyLenOff:])
	pKeyStartOff := types.DecodeFixed[uint32](cfg[IndexTableCfg_V1_PKeyStartOff:])
	pKey := string(cfg[pKeyStartOff : pKeyStartOff+uint32(pKeyLen)])
	return pKey
}

func (cfg IndexTableCfgV1) KeyPart() string {
	keyPartLen := types.DecodeFixed[uint16](cfg[IndexTableCfg_V1_KeyPartLenOff:])
	keyPartStartOff := types.DecodeFixed[uint32](cfg[IndexTableCfg_V1_KeyPartStartOff:])
	keyPart := string(cfg[keyPartStartOff : keyPartStartOff+uint32(keyPartLen)])
	return keyPart
}

func (cfg IndexTableCfgV1) ThreadsSearch() int64 {
	return types.DecodeFixed[int64](cfg[IndexTableCfg_V1_ThreadsSearchOff:])
}

func (cfg IndexTableCfgV1) ThreadsBuild() int64 {
	return types.DecodeFixed[int64](cfg[IndexTableCfg_V1_ThreadsBuildOff:])
}

func (cfg IndexTableCfgV1) IndexCapacity() int64 {
	return types.DecodeFixed[int64](cfg[IndexTableCfg_V1_IndexCapacityOff:])
}

func (cfg IndexTableCfgV1) ExtraCfgType() int16 {
	return types.DecodeFixed[int16](cfg[IndexTableCfg_V1_ExtraCfgTypeOff:])
}

func (cfg IndexTableCfgV1) ExtraIVFCfg() ExtraIVFCfgV1 {
	offset := types.DecodeFixed[uint32](cfg[IndexTableCfg_V1_ExtraCfgStartOff:])
	return ExtraIVFCfgV1(cfg[offset:])
}

func (cfg IndexTableCfgV1) ToMap() map[string]string {
	map1 := map[string]string{
		"db":             cfg.DBName(),
		"src":            cfg.SrcTable(),
		"metadata":       cfg.MetadataTable(),
		"index":          cfg.IndexTable(),
		"pkey":           cfg.PKey(),
		"part":           cfg.KeyPart(),
		"threads_search": fmt.Sprintf("%d", cfg.ThreadsSearch()),
		"threads_build":  fmt.Sprintf("%d", cfg.ThreadsBuild()),
		"index_capacity": fmt.Sprintf("%d", cfg.IndexCapacity()),
	}
	if cfg.ExtraCfgType() == IndexTableCfg_ExtraCfgType_IVFFLAT {
		map2 := cfg.ExtraIVFCfg().ToMap()
		for k, v := range map2 {
			map1[k] = v
		}
	}
	return map1
}

func (cfg IndexTableCfgV1) ToJsonString() string {
	json, err := json.Marshal(cfg.ToMap())
	if err != nil {
		return ""
	}
	return string(json)
}

// ------------------------[ExtraIVFCfgV1] ------------------------

type ExtraIVFCfgV1 []byte

func (cfg ExtraIVFCfgV1) SetFixedValues(
	dataSize int64,
	nprobe uint32,
	pKeyType int32,
	keyPartType int32,
	kmeansTrainPercent int64,
	kmeansMaxIteration int64,
) {
	copy(cfg[ExtraIVFCfg_V1_DataSizeOff:], types.EncodeFixed(dataSize))
	copy(cfg[ExtraIVFCfg_V1_NprobeOff:], types.EncodeFixed(nprobe))
	copy(cfg[ExtraIVFCfg_V1_PKeyTypeOff:], types.EncodeFixed(pKeyType))
	copy(cfg[ExtraIVFCfg_V1_KeyPartTypeOff:], types.EncodeFixed(keyPartType))
	copy(cfg[ExtraIVFCfg_V1_KmeansTrainPercentOff:], types.EncodeFixed(kmeansTrainPercent))
	copy(cfg[ExtraIVFCfg_V1_KmeansMaxIterationOff:], types.EncodeFixed(kmeansMaxIteration))
}

func (cfg ExtraIVFCfgV1) SetEntriesTable(
	entriesTable string,
) {
	entriesTableLen := len(entriesTable)
	copy(cfg[ExtraIVFCfg_V1_EntriesTableLenOff:], types.EncodeFixed(uint16(entriesTableLen)))
	copy(cfg[ExtraIVFCfg_V1_EntriesTableHeaderLen:], entriesTable)
}

func (cfg ExtraIVFCfgV1) EntriesTable() string {
	entriesTableLen := types.DecodeFixed[uint16](cfg[ExtraIVFCfg_V1_EntriesTableLenOff:])
	entriesTable := string(cfg[ExtraIVFCfg_V1_EntriesTableHeaderLen : ExtraIVFCfg_V1_EntriesTableHeaderLen+uint16(entriesTableLen)])
	return entriesTable
}

func (cfg ExtraIVFCfgV1) DataSize() int64 {
	return types.DecodeFixed[int64](cfg[ExtraIVFCfg_V1_DataSizeOff:])
}

func (cfg ExtraIVFCfgV1) Nprobe() uint32 {
	return types.DecodeFixed[uint32](cfg[ExtraIVFCfg_V1_NprobeOff:])
}

func (cfg ExtraIVFCfgV1) PKeyType() int32 {
	return types.DecodeFixed[int32](cfg[ExtraIVFCfg_V1_PKeyTypeOff:])
}

func (cfg ExtraIVFCfgV1) KeyPartType() int32 {
	return types.DecodeFixed[int32](cfg[ExtraIVFCfg_V1_KeyPartTypeOff:])
}

func (cfg ExtraIVFCfgV1) KmeansTrainPercent() int64 {
	return types.DecodeFixed[int64](cfg[ExtraIVFCfg_V1_KmeansTrainPercentOff:])
}

func (cfg ExtraIVFCfgV1) KmeansMaxIteration() int64 {
	return types.DecodeFixed[int64](cfg[ExtraIVFCfg_V1_KmeansMaxIterationOff:])
}

func (cfg ExtraIVFCfgV1) ToMap() map[string]string {
	return map[string]string{
		"datasize":             fmt.Sprintf("%d", cfg.DataSize()),
		"nprobe":               fmt.Sprintf("%d", cfg.Nprobe()),
		"pktype":               fmt.Sprintf("%d", cfg.PKeyType()),
		"parttype":             fmt.Sprintf("%d", cfg.KeyPartType()),
		"kmeans_train_percent": fmt.Sprintf("%d", cfg.KmeansTrainPercent()),
		"kmeans_max_iteration": fmt.Sprintf("%d", cfg.KmeansMaxIteration()),
		"entries":              cfg.EntriesTable(),
	}
}

func JsonStringToIndexTableCfgV1(
	jsonString string,
) (cfg IndexTableCfgV1, err error) {
	map1 := make(map[string]string)
	if err = json.Unmarshal([]byte(jsonString), &map1); err != nil {
		return
	}
	dbName := map1["db"]
	srcTable := map1["src"]
	metadataTable := map1["metadata"]
	indexTable := map1["index"]
	pKey := map1["pkey"]
	keyPart := map1["part"]
	threadsSearchStr := map1["threads_search"]
	threadsBuildStr := map1["threads_build"]
	indexCapacityStr := map1["index_capacity"]
	var (
		threadsSearch int64
		threadsBuild  int64
		indexCapacity int64
	)

	if threadsSearchStr != "" {
		if threadsSearch, err = strconv.ParseInt(threadsSearchStr, 10, 64); err != nil {
			return
		}
	}
	if threadsBuildStr != "" {
		if threadsBuild, err = strconv.ParseInt(threadsBuildStr, 10, 64); err != nil {
			return
		}
	}
	if indexCapacityStr != "" {
		if indexCapacity, err = strconv.ParseInt(indexCapacityStr, 10, 64); err != nil {
			return
		}
	}

	cfg = BuildIndexTableCfgV1(
		dbName,
		srcTable,
		metadataTable,
		indexTable,
		pKey,
		keyPart,
		threadsSearch,
		threadsBuild,
		indexCapacity,
	)
	return
}

func TryeConvertIndexTableCfgV1(
	cfgStr string,
) (cfg IndexTableCfgV1, err error) {
	cfg = IndexTableCfgV1(util.UnsafeStringToBytes(cfgStr))
	if cfg.IsCfgV1() {
		return
	}
	cfg, err = JsonStringToIndexTableCfgV1(cfgStr)
	return
}
