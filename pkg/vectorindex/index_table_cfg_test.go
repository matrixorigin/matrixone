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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ExtraIVFCfgV1(t *testing.T) {
	// different length of table entries
	ivfTableEntries := []string{
		"advnd",
		"dasdaaasd",
		"dasdasdasdasdasdasdasdasdasdasdasdasdasdasda",
	}
	ivfDataSizes := []int64{
		100,
		0,
		10000,
	}

	ivfNprobes := []uint32{
		200,
		0,
		0,
	}

	ivfPKeyTypes := []int32{
		88,
		0,
		1,
	}

	ivfKeyPartTypes := []int32{
		1099,
		32,
		0,
	}

	ivfKmeansTrainPercents := []int64{
		100,
		0,
		10000,
	}

	ivfKmeansMaxIterations := []int64{
		0,
		10000,
		0,
	}

	for i, tableEntry := range ivfTableEntries {
		size := CalculateExtraIVFCfgV1Size(len(tableEntry))
		cfg := ExtraIVFCfgV1(make([]byte, size))
		cfg.SetEntriesTable(tableEntry)
		cfg.SetFixedValues(
			ivfDataSizes[i],
			ivfNprobes[i],
			ivfPKeyTypes[i],
			ivfKeyPartTypes[i],
			ivfKmeansTrainPercents[i],
			ivfKmeansMaxIterations[i],
		)
		require.Equal(t, tableEntry, cfg.EntriesTable())
		require.Equal(t, ivfDataSizes[i], cfg.DataSize())
		require.Equal(t, ivfNprobes[i], cfg.Nprobe())
		require.Equal(t, ivfPKeyTypes[i], cfg.PKeyType())
		require.Equal(t, ivfKeyPartTypes[i], cfg.KeyPartType())
		require.Equal(t, ivfKmeansTrainPercents[i], cfg.KmeansTrainPercent())
		require.Equal(t, ivfKmeansMaxIterations[i], cfg.KmeansMaxIteration())
	}
}

func Test_IndexTableCfgV1(t *testing.T) {
	// 随机生成几组IndexTableCfgV1里面元素的数组列表

	// 随机生成字符串
	randomString := func(n int) string {
		letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
		b := make([]rune, n)
		for i := range b {
			b[i] = letters[rand.Intn(len(letters))]
		}
		return string(b)
	}

	numCases := 5
	dbNames := make([]string, numCases)
	srcTables := make([]string, numCases)
	metadataTables := make([]string, numCases)
	indexTables := make([]string, numCases)
	pKeys := make([]string, numCases)
	keyParts := make([]string, numCases)
	threadsBuilds := make([]int64, numCases)
	threadsSearches := make([]int64, numCases)
	indexCapacities := make([]int64, numCases)
	extraCfgTypes := make([]int16, numCases)
	// extraCfgStarts := make([]uint32, numCases) // unused

	for i := 0; i < numCases; i++ {
		dbNames[i] = randomString(rand.Intn(10) + 3)
		srcTables[i] = randomString(rand.Intn(10) + 3)
		metadataTables[i] = randomString(rand.Intn(10) + 3)
		indexTables[i] = randomString(rand.Intn(10) + 3)
		pKeys[i] = randomString(rand.Intn(5) + 2)
		keyParts[i] = randomString(rand.Intn(5) + 2)
		threadsBuilds[i] = rand.Int63n(100) + 1
		threadsSearches[i] = rand.Int63n(100) + 1
		indexCapacities[i] = rand.Int63n(10000) + 100
		extraCfgTypes[i] = int16(rand.Intn(2))
		// extraCfgStarts[i] = uint32(rand.Intn(1000)) // unused
	}

	for i := 0; i < numCases; i++ {
		size := CalculateIndexTableCfgV1Size(
			len(dbNames[i]),
			len(srcTables[i]),
			len(metadataTables[i]),
			len(indexTables[i]),
			len(pKeys[i]),
			len(keyParts[i]),
		)
		cfg := MakeEmptyIndexTableCfgV1(size, extraCfgTypes[i])
		cfg.SetStringValues(
			dbNames[i],
			srcTables[i],
			metadataTables[i],
			indexTables[i],
			pKeys[i],
			keyParts[i],
		)
		cfg.SetInt64Values(
			threadsSearches[i],
			threadsBuilds[i],
			indexCapacities[i],
		)
		// 修复：增加对MagicNumber的校验
		require.True(t, cfg.IsCfgV1())
		require.Equal(t, dbNames[i], cfg.DBName())
		require.Equal(t, srcTables[i], cfg.SrcTable())
		require.Equal(t, metadataTables[i], cfg.MetadataTable())
		require.Equal(t, indexTables[i], cfg.IndexTable())
		require.Equal(t, pKeys[i], cfg.PKey())
		require.Equal(t, keyParts[i], cfg.KeyPart())
		require.Equal(t, threadsSearches[i], cfg.ThreadsSearch())
		require.Equal(t, threadsBuilds[i], cfg.ThreadsBuild())
		require.Equal(t, indexCapacities[i], cfg.IndexCapacity())
		require.Equal(t, extraCfgTypes[i], cfg.ExtraCfgType())
	}
}

func Test_IndexTableCfgV1_BuildIVFIndexTableCfgV1(t *testing.T) {
	cfg := BuildIVFIndexTableCfgV1(
		"db1",
		"src1",
		"metadata1",
		"index1",
		"pkey1",
		88,
		"keypart1",
		1099,
		100,
		"entries1",
		200,
	)
	require.Equal(t, "db1", cfg.DBName())
	require.Equal(t, "src1", cfg.SrcTable())
	require.Equal(t, "metadata1", cfg.MetadataTable())
	require.Equal(t, "index1", cfg.IndexTable())
	require.Equal(t, "pkey1", cfg.PKey())
	require.Equal(t, "keypart1", cfg.KeyPart())
	require.Equal(t, int64(100), cfg.ThreadsSearch())
	require.Equal(t, int64(0), cfg.ThreadsBuild())
	require.Equal(t, int64(0), cfg.IndexCapacity())
	require.Equal(t, int16(IndexTableCfg_ExtraCfgType_IVFFLAT), cfg.ExtraCfgType())
	require.Equal(t, "entries1", cfg.ExtraIVFCfg().EntriesTable())
	require.Equal(t, uint32(200), cfg.ExtraIVFCfg().Nprobe())
	require.Equal(t, int32(88), cfg.ExtraIVFCfg().PKeyType())
	require.Equal(t, int32(1099), cfg.ExtraIVFCfg().KeyPartType())
	require.Equal(t, int64(100), cfg.ExtraIVFCfg().KmeansTrainPercent())
	require.Equal(t, int64(0), cfg.ExtraIVFCfg().KmeansMaxIteration())
}
