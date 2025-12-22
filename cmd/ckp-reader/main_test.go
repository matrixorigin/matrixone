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

package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func TestGetTypeName(t *testing.T) {
	tests := []struct {
		name     string
		typ      int8
		expected string
	}{
		{"ET_Global", 0, "ET_Global"},
		{"ET_Incremental", 1, "ET_Incremental"},
		{"ET_Compacted", 2, "ET_Compacted"},
		{"ET_Backup", 3, "ET_Backup"},
		{"Unknown", 99, "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getTypeName(tt.typ)
			if result != tt.expected {
				t.Errorf("getTypeName(%d) = %s, want %s", tt.typ, result, tt.expected)
			}
		})
	}
}

func TestFindRowByEndTS(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	if err != nil {
		t.Fatalf("failed to create mpool: %v", err)
	}
	defer mpool.DeleteMPool(mp)

	// Create a test batch with 3 rows
	bat := batch.NewWithSize(2) // start_ts and end_ts columns
	bat.SetRowCount(3)
	// Column 0: start_ts
	startTSVec := vector.NewVec(types.T_TS.ToType())
	ts1 := types.BuildTS(1, 0)
	ts2 := types.BuildTS(2, 0)
	ts3 := types.BuildTS(3, 0)
	vector.AppendFixed(startTSVec, ts1, false, mp)
	vector.AppendFixed(startTSVec, ts2, false, mp)
	vector.AppendFixed(startTSVec, ts3, false, mp)
	bat.Vecs[0] = startTSVec

	// Column 1: end_ts
	endTSVec := vector.NewVec(types.T_TS.ToType())
	endTS1 := types.BuildTS(10, 0)
	endTS2 := types.BuildTS(20, 0)
	endTS3 := types.BuildTS(30, 0)
	vector.AppendFixed(endTSVec, endTS1, false, mp)
	vector.AppendFixed(endTSVec, endTS2, false, mp)
	vector.AppendFixed(endTSVec, endTS3, false, mp)
	bat.Vecs[1] = endTSVec

	tests := []struct {
		name        string
		targetEndTS string
		expected    int
	}{
		{"Find first row", endTS1.ToString(), 0},
		{"Find second row", endTS2.ToString(), 1},
		{"Find third row", endTS3.ToString(), 2},
		{"Not found", "999-0", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findRowByEndTS(bat, tt.targetEndTS)
			if result != tt.expected {
				t.Errorf("findRowByEndTS(%s) = %d, want %d", tt.targetEndTS, result, tt.expected)
			}
		})
	}
}

func TestCreateBatchWithoutRow(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	if err != nil {
		t.Fatalf("failed to create mpool: %v", err)
	}
	defer mpool.DeleteMPool(mp)

	// Create a test batch with 3 rows
	bat := batch.NewWithSize(2) // start_ts and end_ts columns
	bat.SetRowCount(3)

	// Column 0: start_ts
	startTSVec := vector.NewVec(types.T_TS.ToType())
	ts1 := types.BuildTS(1, 0)
	ts2 := types.BuildTS(2, 0)
	ts3 := types.BuildTS(3, 0)
	vector.AppendFixed(startTSVec, ts1, false, mp)
	vector.AppendFixed(startTSVec, ts2, false, mp)
	vector.AppendFixed(startTSVec, ts3, false, mp)
	bat.Vecs[0] = startTSVec

	// Column 1: end_ts
	endTSVec := vector.NewVec(types.T_TS.ToType())
	endTS1 := types.BuildTS(10, 0)
	endTS2 := types.BuildTS(20, 0)
	endTS3 := types.BuildTS(30, 0)
	vector.AppendFixed(endTSVec, endTS1, false, mp)
	vector.AppendFixed(endTSVec, endTS2, false, mp)
	vector.AppendFixed(endTSVec, endTS3, false, mp)
	bat.Vecs[1] = endTSVec

	// Test deleting row 1 (middle row)
	newBat, err := createBatchWithoutRow(bat, 1, mp)
	if err != nil {
		t.Fatalf("createBatchWithoutRow failed: %v", err)
	}

	if newBat.RowCount() != 2 {
		t.Errorf("Expected 2 rows, got %d", newBat.RowCount())
	}

	// Verify the remaining rows
	// Row 0 should be the original row 0
	remainingTS1 := types.DecodeFixed[types.TS](newBat.Vecs[0].GetRawBytesAt(0))
	if remainingTS1 != ts1 {
		t.Errorf("Row 0 start_ts = %s, want %s", remainingTS1.ToString(), ts1.ToString())
	}

	// Row 1 should be the original row 2
	remainingTS2 := types.DecodeFixed[types.TS](newBat.Vecs[0].GetRawBytesAt(1))
	if remainingTS2 != ts3 {
		t.Errorf("Row 1 start_ts = %s, want %s", remainingTS2.ToString(), ts3.ToString())
	}
}

func TestCreateFileService(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	tests := []struct {
		name      string
		useS3Mode bool
		wantErr   bool
	}{
		{"S3 mode", true, false},
		{"LocalFS mode", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, err := createFileService(ctx, tmpDir, tt.useS3Mode)
			if (err != nil) != tt.wantErr {
				t.Errorf("createFileService() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if fs != nil {
				fs.Close(ctx)
			}
		})
	}
}

func TestWriteBatchToFile(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	if err != nil {
		t.Fatalf("failed to create mpool: %v", err)
	}
	defer mpool.DeleteMPool(mp)

	// Create a test batch
	bat := batch.NewWithSize(2)
	bat.SetRowCount(1)

	startTSVec := vector.NewVec(types.T_TS.ToType())
	ts := types.BuildTS(1, 0)
	vector.AppendFixed(startTSVec, ts, false, mp)
	bat.Vecs[0] = startTSVec

	endTSVec := vector.NewVec(types.T_TS.ToType())
	endTS := types.BuildTS(10, 0)
	vector.AppendFixed(endTSVec, endTS, false, mp)
	bat.Vecs[1] = endTSVec

	// Create file service
	fs, err := createFileService(ctx, tmpDir, true)
	if err != nil {
		t.Fatalf("failed to create file service: %v", err)
	}
	defer fs.Close(ctx)

	// Write batch to file
	fileName := "test_ckp.ckp"
	if err := writeBatchToFile(ctx, fs, fileName, bat); err != nil {
		t.Fatalf("writeBatchToFile failed: %v", err)
	}

	// Verify file exists by trying to read it back
	filePath := filepath.Join(tmpDir, fileName)
	if err := readCkpFile(filePath, true); err != nil {
		t.Errorf("File was not created or cannot be read: %v", err)
	}
}

func TestReadCkpFileIntegration(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	if err != nil {
		t.Fatalf("failed to create mpool: %v", err)
	}
	defer mpool.DeleteMPool(mp)

	// Create a test checkpoint file
	bat := batch.NewWithSize(2)
	bat.SetRowCount(2)

	startTSVec := vector.NewVec(types.T_TS.ToType())
	ts1 := types.BuildTS(1, 0)
	ts2 := types.BuildTS(2, 0)
	vector.AppendFixed(startTSVec, ts1, false, mp)
	vector.AppendFixed(startTSVec, ts2, false, mp)
	bat.Vecs[0] = startTSVec

	endTSVec := vector.NewVec(types.T_TS.ToType())
	endTS1 := types.BuildTS(10, 0)
	endTS2 := types.BuildTS(20, 0)
	vector.AppendFixed(endTSVec, endTS1, false, mp)
	vector.AppendFixed(endTSVec, endTS2, false, mp)
	bat.Vecs[1] = endTSVec

	// Create file service and write
	fs, err := createFileService(ctx, tmpDir, true)
	if err != nil {
		t.Fatalf("failed to create file service: %v", err)
	}
	defer fs.Close(ctx)

	fileName := "test_ckp.ckp"
	if err := writeBatchToFile(ctx, fs, fileName, bat); err != nil {
		t.Fatalf("writeBatchToFile failed: %v", err)
	}

	// Read the file back
	filePath := filepath.Join(tmpDir, fileName)
	if err := readCkpFile(filePath, true); err != nil {
		t.Fatalf("readCkpFile failed: %v", err)
	}
}

func TestDeleteRowFromCkpFileIntegration(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	if err != nil {
		t.Fatalf("failed to create mpool: %v", err)
	}
	defer mpool.DeleteMPool(mp)

	// Create a test checkpoint file with 3 rows
	bat := batch.NewWithSize(2)
	bat.SetRowCount(3)

	startTSVec := vector.NewVec(types.T_TS.ToType())
	ts1 := types.BuildTS(1, 0)
	ts2 := types.BuildTS(2, 0)
	ts3 := types.BuildTS(3, 0)
	vector.AppendFixed(startTSVec, ts1, false, mp)
	vector.AppendFixed(startTSVec, ts2, false, mp)
	vector.AppendFixed(startTSVec, ts3, false, mp)
	bat.Vecs[0] = startTSVec

	endTSVec := vector.NewVec(types.T_TS.ToType())
	endTS1 := types.BuildTS(10, 0)
	endTS2 := types.BuildTS(20, 0)
	endTS3 := types.BuildTS(30, 0)
	vector.AppendFixed(endTSVec, endTS1, false, mp)
	vector.AppendFixed(endTSVec, endTS2, false, mp)
	vector.AppendFixed(endTSVec, endTS3, false, mp)
	bat.Vecs[1] = endTSVec

	// Create file service and write
	fs, err := createFileService(ctx, tmpDir, true)
	if err != nil {
		t.Fatalf("failed to create file service: %v", err)
	}
	defer fs.Close(ctx)

	inputFileName := "test_input.ckp"
	if err := writeBatchToFile(ctx, fs, inputFileName, bat); err != nil {
		t.Fatalf("writeBatchToFile failed: %v", err)
	}

	// Delete row 1
	inputPath := filepath.Join(tmpDir, inputFileName)
	outputPath := filepath.Join(tmpDir, "test_output.ckp")
	if err := deleteRowFromCkpFile(inputPath, 1, outputPath, true); err != nil {
		t.Fatalf("deleteRowFromCkpFile failed: %v", err)
	}

	// Verify the output file exists and can be read
	readFs, err := createFileService(ctx, tmpDir, true)
	if err != nil {
		t.Fatalf("failed to create read file service: %v", err)
	}
	defer readFs.Close(ctx)

	// Verify by reading the file back
	if err := readCkpFile(outputPath, true); err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
}
