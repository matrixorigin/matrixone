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
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
)

const (
	CheckpointAttr_StartTS       = "start_ts"
	CheckpointAttr_EndTS         = "end_ts"
	CheckpointAttr_MetaLocation  = "meta_location"
	CheckpointAttr_EntryType     = "entry_type"
	CheckpointAttr_Version       = "version"
	CheckpointAttr_AllLocations  = "all_locations"
	CheckpointAttr_CheckpointLSN = "checkpoint_lsn"
	CheckpointAttr_TruncateLSN   = "truncate_lsn"
	CheckpointAttr_Type          = "type"
)

func main() {
	filePath := flag.String("file", "", "path to the ckp file")
	useS3Mode := flag.Bool("s3", true, "use S3 mode (DISK backend) to skip checksum validation")
	deleteRow := flag.Int("delete", -1, "row index to delete (0-based)")
	matchEndTS := flag.String("match-end-ts", "", "delete row matching this end_ts value (e.g., '1766148146913695795-1')")
	outputFile := flag.String("output", "", "output file path (default: overwrite original)")
	flag.Parse()

	if *filePath == "" {
		fmt.Println("Usage: ckp-reader -file <path-to-ckp-file> [-s3=true|false] [-delete <row>] [-match-end-ts <ts>] [-output <file>]")
		fmt.Println("  -file:         path to the ckp file")
		fmt.Println("  -s3:           use S3 mode (DISK backend) to skip checksum validation (default: true)")
		fmt.Println("  -delete:       row index to delete (0-based)")
		fmt.Println("  -match-end-ts: delete row matching this end_ts value")
		fmt.Println("  -output:       output file path (default: overwrite original)")
		os.Exit(1)
	}

	output := *outputFile
	if output == "" {
		output = *filePath
	}

	if *matchEndTS != "" {
		if err := deleteRowByEndTS(*filePath, *matchEndTS, output, *useS3Mode); err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
		// After delete, read and verify
		fmt.Println("\n" + strings.Repeat("=", 80))
		fmt.Println("Verifying result...")
		fmt.Println(strings.Repeat("=", 80))
		if err := readCkpFile(output, *useS3Mode); err != nil {
			fmt.Printf("Verification Error: %v\n", err)
			os.Exit(1)
		}
	} else if *deleteRow >= 0 {
		if err := deleteRowFromCkpFile(*filePath, *deleteRow, output, *useS3Mode); err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
		// After delete, read and verify
		fmt.Println("\n" + strings.Repeat("=", 80))
		fmt.Println("Verifying result...")
		fmt.Println(strings.Repeat("=", 80))
		if err := readCkpFile(output, *useS3Mode); err != nil {
			fmt.Printf("Verification Error: %v\n", err)
			os.Exit(1)
		}
	} else {
		if err := readCkpFile(*filePath, *useS3Mode); err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
	}
}

func readCkpFile(filePath string, useS3Mode bool) error {
	ctx := context.Background()

	// Get the directory and filename
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}
	dir := filepath.Dir(absPath)
	fileName := filepath.Base(absPath)

	fmt.Printf("Reading file: %s\n", absPath)
	fmt.Printf("Directory: %s\n", dir)
	fmt.Printf("Filename: %s\n", fileName)
	fmt.Printf("Mode: %s\n", map[bool]string{true: "S3 (DISK backend, no checksum)", false: "LocalFS (with checksum)"}[useS3Mode])
	fmt.Println(strings.Repeat("=", 80))

	var fs fileservice.FileService
	if useS3Mode {
		// Use S3FS with DISK endpoint to skip checksum validation
		args := fileservice.ObjectStorageArguments{
			Name:               "SHARED",
			Endpoint:           "DISK",
			Bucket:             dir,
			NoBucketValidation: true,
		}
		fs, err = fileservice.NewS3FS(ctx, args, fileservice.DisabledCacheConfig, nil, true, true)
		if err != nil {
			return fmt.Errorf("failed to create S3 fs (DISK mode): %w", err)
		}
	} else {
		// Use LocalFS with checksum validation
		fs, err = fileservice.NewLocalFS(ctx, "local", dir, fileservice.DisabledCacheConfig, nil)
		if err != nil {
			return fmt.Errorf("failed to create local fs: %w", err)
		}
	}
	defer fs.Close(ctx)

	// Create a file reader
	reader, err := ioutil.NewFileReader(fs, fileName)
	if err != nil {
		return fmt.Errorf("failed to create file reader: %w", err)
	}

	// Load all columns
	bats, closeCB, err := reader.LoadAllColumns(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}
	defer func() {
		if closeCB != nil {
			closeCB()
		}
	}()

	if len(bats) == 0 {
		fmt.Println("No data in file")
		return nil
	}

	fmt.Printf("Total batches: %d\n", len(bats))

	for batchIdx, bat := range bats {
		fmt.Printf("\n--- Batch %d ---\n", batchIdx)
		fmt.Printf("Columns: %d, Rows: %d\n", len(bat.Vecs), bat.RowCount())

		for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
			fmt.Printf("\n  Row %d:\n", rowIdx)

			// Column 0: start_ts (types.TS)
			if len(bat.Vecs) > 0 && bat.Vecs[0].Length() > rowIdx {
				startTS := types.DecodeFixed[types.TS](bat.Vecs[0].GetRawBytesAt(rowIdx))
				fmt.Printf("    [0] %s: %s\n", CheckpointAttr_StartTS, startTS.ToString())
			}

			// Column 1: end_ts (types.TS)
			if len(bat.Vecs) > 1 && bat.Vecs[1].Length() > rowIdx {
				endTS := types.DecodeFixed[types.TS](bat.Vecs[1].GetRawBytesAt(rowIdx))
				fmt.Printf("    [1] %s: %s\n", CheckpointAttr_EndTS, endTS.ToString())
			}

			// Column 2: meta_location (varchar/[]byte)
			if len(bat.Vecs) > 2 && bat.Vecs[2].Length() > rowIdx {
				metaLoc := bat.Vecs[2].GetBytesAt(rowIdx)
				loc := objectio.Location(metaLoc)
				fmt.Printf("    [2] %s: %s (len=%d)\n", CheckpointAttr_MetaLocation, loc.String(), len(metaLoc))
			}

			// Column 3: entry_type (bool)
			if len(bat.Vecs) > 3 && bat.Vecs[3].Length() > rowIdx {
				entryType := types.DecodeFixed[bool](bat.Vecs[3].GetRawBytesAt(rowIdx))
				fmt.Printf("    [3] %s: %v (true=incremental, false=global)\n", CheckpointAttr_EntryType, entryType)
			}

			// Column 4: version (uint32)
			if len(bat.Vecs) > 4 && bat.Vecs[4].Length() > rowIdx {
				version := types.DecodeFixed[uint32](bat.Vecs[4].GetRawBytesAt(rowIdx))
				fmt.Printf("    [4] %s: %d\n", CheckpointAttr_Version, version)
			}

			// Column 5: all_locations (varchar/[]byte)
			if len(bat.Vecs) > 5 && bat.Vecs[5].Length() > rowIdx {
				allLocs := bat.Vecs[5].GetBytesAt(rowIdx)
				loc := objectio.Location(allLocs)
				fmt.Printf("    [5] %s: %s (len=%d)\n", CheckpointAttr_AllLocations, loc.String(), len(allLocs))
			}

			// Column 6: checkpoint_lsn (uint64)
			if len(bat.Vecs) > 6 && bat.Vecs[6].Length() > rowIdx {
				ckpLSN := types.DecodeFixed[uint64](bat.Vecs[6].GetRawBytesAt(rowIdx))
				fmt.Printf("    [6] %s: %d\n", CheckpointAttr_CheckpointLSN, ckpLSN)
			}

			// Column 7: truncate_lsn (uint64)
			if len(bat.Vecs) > 7 && bat.Vecs[7].Length() > rowIdx {
				truncateLSN := types.DecodeFixed[uint64](bat.Vecs[7].GetRawBytesAt(rowIdx))
				fmt.Printf("    [7] %s: %d\n", CheckpointAttr_TruncateLSN, truncateLSN)
			}

			// Column 8: type (int8)
			if len(bat.Vecs) > 8 && bat.Vecs[8].Length() > rowIdx {
				typ := types.DecodeFixed[int8](bat.Vecs[8].GetRawBytesAt(rowIdx))
				typeName := getTypeName(typ)
				fmt.Printf("    [8] %s: %d (%s)\n", CheckpointAttr_Type, typ, typeName)
			}
		}
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("Done!")
	return nil
}

func getTypeName(typ int8) string {
	// Based on EntryType definitions
	switch typ {
	case 0:
		return "ET_Global"
	case 1:
		return "ET_Incremental"
	case 2:
		return "ET_Compacted"
	case 3:
		return "ET_Backup"
	default:
		return "Unknown"
	}
}

func deleteRowByEndTS(inputPath string, targetEndTS string, outputPath string, useS3Mode bool) error {
	ctx := context.Background()
	mp, err := mpool.NewMPool("ckp-reader", 0, mpool.NoFixed)
	if err != nil {
		return fmt.Errorf("failed to create mpool: %w", err)
	}
	defer mpool.DeleteMPool(mp)

	// Get absolute paths
	absInputPath, err := filepath.Abs(inputPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}
	absOutputPath, err := filepath.Abs(outputPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute output path: %w", err)
	}

	inputDir := filepath.Dir(absInputPath)
	inputFileName := filepath.Base(absInputPath)
	outputDir := filepath.Dir(absOutputPath)
	outputFileName := filepath.Base(absOutputPath)

	fmt.Printf("Input file: %s\n", absInputPath)
	fmt.Printf("Output file: %s\n", absOutputPath)
	fmt.Printf("Matching end_ts: %s\n", targetEndTS)
	fmt.Println(strings.Repeat("=", 80))

	// Create file service for reading
	var readFs fileservice.FileService
	if useS3Mode {
		args := fileservice.ObjectStorageArguments{
			Name:               "SHARED",
			Endpoint:           "DISK",
			Bucket:             inputDir,
			NoBucketValidation: true,
		}
		readFs, err = fileservice.NewS3FS(ctx, args, fileservice.DisabledCacheConfig, nil, true, true)
		if err != nil {
			return fmt.Errorf("failed to create S3 fs for reading: %w", err)
		}
	} else {
		readFs, err = fileservice.NewLocalFS(ctx, "local", inputDir, fileservice.DisabledCacheConfig, nil)
		if err != nil {
			return fmt.Errorf("failed to create local fs for reading: %w", err)
		}
	}
	defer readFs.Close(ctx)

	// Read the file
	reader, err := ioutil.NewFileReader(readFs, inputFileName)
	if err != nil {
		return fmt.Errorf("failed to create file reader: %w", err)
	}

	bats, closeCB, err := reader.LoadAllColumns(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}
	defer func() {
		if closeCB != nil {
			closeCB()
		}
	}()

	if len(bats) == 0 {
		return fmt.Errorf("no data in file")
	}

	srcBat := bats[0]
	totalRows := srcBat.RowCount()

	fmt.Printf("Original row count: %d\n", totalRows)

	// Find the row to delete by matching end_ts
	rowToDelete := -1
	for rowIdx := 0; rowIdx < totalRows; rowIdx++ {
		if len(srcBat.Vecs) > 1 && srcBat.Vecs[1].Length() > rowIdx {
			endTS := types.DecodeFixed[types.TS](srcBat.Vecs[1].GetRawBytesAt(rowIdx))
			if endTS.ToString() == targetEndTS {
				rowToDelete = rowIdx
				fmt.Printf("Found matching row at index %d with end_ts: %s\n", rowIdx, endTS.ToString())
				break
			}
		}
	}

	if rowToDelete < 0 {
		return fmt.Errorf("no row found with end_ts = %s", targetEndTS)
	}

	// Create a new batch without the deleted row
	newBat := batch.NewWithSize(len(srcBat.Vecs))
	newBat.SetRowCount(totalRows - 1)

	// Copy vectors, skipping the deleted row
	for colIdx := 0; colIdx < len(srcBat.Vecs); colIdx++ {
		srcVec := srcBat.Vecs[colIdx]
		newVec := vector.NewVec(*srcVec.GetType())

		for rowIdx := 0; rowIdx < totalRows; rowIdx++ {
			if rowIdx == rowToDelete {
				continue
			}
			if err := newVec.UnionOne(srcVec, int64(rowIdx), mp); err != nil {
				return fmt.Errorf("failed to copy row %d col %d: %w", rowIdx, colIdx, err)
			}
		}
		newBat.Vecs[colIdx] = newVec
	}
	newBat.SetRowCount(totalRows - 1)

	fmt.Printf("New row count: %d\n", newBat.RowCount())

	// Create file service for writing
	var writeFs fileservice.FileService
	if useS3Mode {
		args := fileservice.ObjectStorageArguments{
			Name:               "SHARED",
			Endpoint:           "DISK",
			Bucket:             outputDir,
			NoBucketValidation: true,
		}
		writeFs, err = fileservice.NewS3FS(ctx, args, fileservice.DisabledCacheConfig, nil, true, true)
		if err != nil {
			return fmt.Errorf("failed to create S3 fs for writing: %w", err)
		}
	} else {
		writeFs, err = fileservice.NewLocalFS(ctx, "local", outputDir, fileservice.DisabledCacheConfig, nil)
		if err != nil {
			return fmt.Errorf("failed to create local fs for writing: %w", err)
		}
	}
	defer writeFs.Close(ctx)

	// Delete existing file if overwriting
	if absInputPath == absOutputPath {
		if err := writeFs.Delete(ctx, outputFileName); err != nil {
			fmt.Printf("Warning: failed to delete existing file: %v\n", err)
		}
	}

	// Write the new file
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, outputFileName, writeFs)
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}

	if _, err = writer.Write(newBat); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	if _, err = writer.WriteEnd(ctx); err != nil {
		return fmt.Errorf("failed to finalize write: %w", err)
	}

	fmt.Printf("Successfully deleted row with end_ts=%s and wrote to %s\n", targetEndTS, absOutputPath)
	return nil
}

func deleteRowFromCkpFile(inputPath string, rowToDelete int, outputPath string, useS3Mode bool) error {
	ctx := context.Background()
	mp, err := mpool.NewMPool("ckp-reader", 0, mpool.NoFixed)
	if err != nil {
		return fmt.Errorf("failed to create mpool: %w", err)
	}
	defer mpool.DeleteMPool(mp)

	// Get absolute paths
	absInputPath, err := filepath.Abs(inputPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}
	absOutputPath, err := filepath.Abs(outputPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute output path: %w", err)
	}

	inputDir := filepath.Dir(absInputPath)
	inputFileName := filepath.Base(absInputPath)
	outputDir := filepath.Dir(absOutputPath)
	outputFileName := filepath.Base(absOutputPath)

	fmt.Printf("Input file: %s\n", absInputPath)
	fmt.Printf("Output file: %s\n", absOutputPath)
	fmt.Printf("Deleting row: %d\n", rowToDelete)
	fmt.Println(strings.Repeat("=", 80))

	// Create file service for reading
	var readFs fileservice.FileService
	if useS3Mode {
		args := fileservice.ObjectStorageArguments{
			Name:               "SHARED",
			Endpoint:           "DISK",
			Bucket:             inputDir,
			NoBucketValidation: true,
		}
		readFs, err = fileservice.NewS3FS(ctx, args, fileservice.DisabledCacheConfig, nil, true, true)
		if err != nil {
			return fmt.Errorf("failed to create S3 fs for reading: %w", err)
		}
	} else {
		readFs, err = fileservice.NewLocalFS(ctx, "local", inputDir, fileservice.DisabledCacheConfig, nil)
		if err != nil {
			return fmt.Errorf("failed to create local fs for reading: %w", err)
		}
	}
	defer readFs.Close(ctx)

	// Read the file
	reader, err := ioutil.NewFileReader(readFs, inputFileName)
	if err != nil {
		return fmt.Errorf("failed to create file reader: %w", err)
	}

	bats, closeCB, err := reader.LoadAllColumns(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to load columns: %w", err)
	}
	defer func() {
		if closeCB != nil {
			closeCB()
		}
	}()

	if len(bats) == 0 {
		return fmt.Errorf("no data in file")
	}

	// We assume there's only one batch (typical for ckp meta files)
	srcBat := bats[0]
	totalRows := srcBat.RowCount()

	fmt.Printf("Original row count: %d\n", totalRows)

	if rowToDelete < 0 || rowToDelete >= totalRows {
		return fmt.Errorf("row index %d out of range [0, %d)", rowToDelete, totalRows)
	}

	// Create a new batch without the deleted row
	newBat := batch.NewWithSize(len(srcBat.Vecs))
	newBat.SetRowCount(totalRows - 1)

	// Copy vectors, skipping the deleted row
	for colIdx := 0; colIdx < len(srcBat.Vecs); colIdx++ {
		srcVec := srcBat.Vecs[colIdx]
		newVec := vector.NewVec(*srcVec.GetType())

		for rowIdx := 0; rowIdx < totalRows; rowIdx++ {
			if rowIdx == rowToDelete {
				continue
			}
			if err := newVec.UnionOne(srcVec, int64(rowIdx), mp); err != nil {
				return fmt.Errorf("failed to copy row %d col %d: %w", rowIdx, colIdx, err)
			}
		}
		newBat.Vecs[colIdx] = newVec
	}
	newBat.SetRowCount(totalRows - 1)

	fmt.Printf("New row count: %d\n", newBat.RowCount())

	// Create file service for writing
	var writeFs fileservice.FileService
	if useS3Mode {
		args := fileservice.ObjectStorageArguments{
			Name:               "SHARED",
			Endpoint:           "DISK",
			Bucket:             outputDir,
			NoBucketValidation: true,
		}
		writeFs, err = fileservice.NewS3FS(ctx, args, fileservice.DisabledCacheConfig, nil, true, true)
		if err != nil {
			return fmt.Errorf("failed to create S3 fs for writing: %w", err)
		}
	} else {
		writeFs, err = fileservice.NewLocalFS(ctx, "local", outputDir, fileservice.DisabledCacheConfig, nil)
		if err != nil {
			return fmt.Errorf("failed to create local fs for writing: %w", err)
		}
	}
	defer writeFs.Close(ctx)

	// Delete existing file if overwriting
	if absInputPath == absOutputPath {
		if err := writeFs.Delete(ctx, outputFileName); err != nil {
			fmt.Printf("Warning: failed to delete existing file: %v\n", err)
		}
	}

	// Write the new file
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, outputFileName, writeFs)
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}

	if _, err = writer.Write(newBat); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	if _, err = writer.WriteEnd(ctx); err != nil {
		return fmt.Errorf("failed to finalize write: %w", err)
	}

	fmt.Printf("Successfully deleted row %d and wrote to %s\n", rowToDelete, absOutputPath)
	return nil
}
