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
	"log"
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

const (
	cmdRead   = "read"
	cmdDelete = "delete"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	switch command {
	case cmdRead:
		runReadCommand()
	case cmdDelete:
		runDeleteCommand()
	case "help", "-h", "--help":
		printUsage()
		os.Exit(0)
	default:
		log.Printf("Error: unknown command '%s'\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	log.Println("ckp-tool - A tool for reading and modifying checkpoint meta files")
	log.Println()
	log.Println("Usage:")
	log.Println("  ckp-tool <command> [flags]")
	log.Println()
	log.Println("Commands:")
	log.Println("  read      Read and display checkpoint meta file contents")
	log.Println("  delete    Delete rows from checkpoint meta file")
	log.Println("  help      Show this help message")
	log.Println()
	log.Println("Read Command:")
	log.Println("  ckp-tool read -file <path> [-s3=true|false]")
	log.Println()
	log.Println("    Flags:")
	log.Println("      -file string    Path to the checkpoint meta file (required)")
	log.Println("      -s3 bool        Use S3 mode (DISK backend) to skip checksum validation (default: true)")
	log.Println()
	log.Println("Delete Command:")
	log.Println("  ckp-tool delete -file <path> [-index <row> | -end-ts <ts>] [-output <path>] [-s3=true|false]")
	log.Println()
	log.Println("    Flags:")
	log.Println("      -file string    Path to the checkpoint meta file (required)")
	log.Println("      -index int     Row index to delete (0-based, mutually exclusive with -end-ts)")
	log.Println("      -end-ts string Delete row matching this end_ts value (e.g., '1766148146913695795-1')")
	log.Println("      -output string Output file path (default: overwrite original file)")
	log.Println("      -s3 bool       Use S3 mode (DISK backend) to skip checksum validation (default: true)")
	log.Println()
	log.Println("Examples:")
	log.Println("  # Read a checkpoint file")
	log.Println("  ckp-tool read -file ./meta_123_456.ckp")
	log.Println()
	log.Println("  # Delete row by index")
	log.Println("  ckp-tool delete -file ./meta_123_456.ckp -index 0")
	log.Println()
	log.Println("  # Delete row by end_ts")
	log.Println("  ckp-tool delete -file ./meta_123_456.ckp -end-ts '1766148146913695795-1' -output ./meta_new.ckp")
	log.Println()
}

func runReadCommand() {
	readFlags := flag.NewFlagSet("read", flag.ExitOnError)
	filePath := readFlags.String("file", "", "path to the ckp file")
	useS3Mode := readFlags.Bool("s3", true, "use S3 mode (DISK backend) to skip checksum validation")
	readFlags.Parse(os.Args[2:])

	if *filePath == "" {
		log.Println("Error: -file flag is required")
		readFlags.Usage()
		os.Exit(1)
	}

	if err := readCkpFile(*filePath, *useS3Mode); err != nil {
		log.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func runDeleteCommand() {
	deleteFlags := flag.NewFlagSet("delete", flag.ExitOnError)
	filePath := deleteFlags.String("file", "", "path to the ckp file")
	deleteIndex := deleteFlags.Int("index", -1, "row index to delete (0-based)")
	matchEndTS := deleteFlags.String("end-ts", "", "delete row matching this end_ts value")
	outputFile := deleteFlags.String("output", "", "output file path (default: overwrite original)")
	useS3Mode := deleteFlags.Bool("s3", true, "use S3 mode (DISK backend) to skip checksum validation")
	deleteFlags.Parse(os.Args[2:])

	if *filePath == "" {
		log.Println("Error: -file flag is required")
		deleteFlags.Usage()
		os.Exit(1)
	}

	if *deleteIndex >= 0 && *matchEndTS != "" {
		log.Println("Error: -index and -end-ts are mutually exclusive")
		deleteFlags.Usage()
		os.Exit(1)
	}

	if *deleteIndex < 0 && *matchEndTS == "" {
		log.Println("Error: either -index or -end-ts must be specified")
		deleteFlags.Usage()
		os.Exit(1)
	}

	output := *outputFile
	if output == "" {
		output = *filePath
	}

	var err error
	if *matchEndTS != "" {
		err = deleteRowByEndTS(*filePath, *matchEndTS, output, *useS3Mode)
	} else {
		err = deleteRowFromCkpFile(*filePath, *deleteIndex, output, *useS3Mode)
	}

	if err != nil {
		log.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	// After delete, read and verify
	log.Println("\n" + strings.Repeat("=", 80))
	log.Println("Verifying result...")
	log.Println(strings.Repeat("=", 80))
	if err := readCkpFile(output, *useS3Mode); err != nil {
		log.Printf("Verification Error: %v\n", err)
		os.Exit(1)
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

	log.Printf("Reading file: %s\n", absPath)
	log.Printf("Directory: %s\n", dir)
	log.Printf("Filename: %s\n", fileName)
	log.Printf("Mode: %s\n", map[bool]string{true: "S3 (DISK backend, no checksum)", false: "LocalFS (with checksum)"}[useS3Mode])
	log.Println(strings.Repeat("=", 80))

	fs, err := createFileService(ctx, dir, useS3Mode)
	if err != nil {
		return err
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
		log.Println("No data in file")
		return nil
	}

	log.Printf("Total batches: %d\n", len(bats))

	for batchIdx, bat := range bats {
		log.Printf("\n--- Batch %d ---\n", batchIdx)
		log.Printf("Columns: %d, Rows: %d\n", len(bat.Vecs), bat.RowCount())

		for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
			printRow(bat, rowIdx)
		}
	}

	log.Println("\n" + strings.Repeat("=", 80))
	log.Println("Done!")
	return nil
}

func printRow(bat *batch.Batch, rowIdx int) {
	log.Printf("\n  Row %d:\n", rowIdx)

	// Column 0: start_ts (types.TS)
	if len(bat.Vecs) > 0 && bat.Vecs[0].Length() > rowIdx {
		startTS := types.DecodeFixed[types.TS](bat.Vecs[0].GetRawBytesAt(rowIdx))
		log.Printf("    [0] %s: %s\n", CheckpointAttr_StartTS, startTS.ToString())
	}

	// Column 1: end_ts (types.TS)
	if len(bat.Vecs) > 1 && bat.Vecs[1].Length() > rowIdx {
		endTS := types.DecodeFixed[types.TS](bat.Vecs[1].GetRawBytesAt(rowIdx))
		log.Printf("    [1] %s: %s\n", CheckpointAttr_EndTS, endTS.ToString())
	}

	// Column 2: meta_location (varchar/[]byte)
	if len(bat.Vecs) > 2 && bat.Vecs[2].Length() > rowIdx {
		metaLoc := bat.Vecs[2].GetBytesAt(rowIdx)
		loc := objectio.Location(metaLoc)
		log.Printf("    [2] %s: %s (len=%d)\n", CheckpointAttr_MetaLocation, loc.String(), len(metaLoc))
	}

	// Column 3: entry_type (bool)
	if len(bat.Vecs) > 3 && bat.Vecs[3].Length() > rowIdx {
		entryType := types.DecodeFixed[bool](bat.Vecs[3].GetRawBytesAt(rowIdx))
		log.Printf("    [3] %s: %v (true=incremental, false=global)\n", CheckpointAttr_EntryType, entryType)
	}

	// Column 4: version (uint32)
	if len(bat.Vecs) > 4 && bat.Vecs[4].Length() > rowIdx {
		version := types.DecodeFixed[uint32](bat.Vecs[4].GetRawBytesAt(rowIdx))
		log.Printf("    [4] %s: %d\n", CheckpointAttr_Version, version)
	}

	// Column 5: all_locations (varchar/[]byte)
	if len(bat.Vecs) > 5 && bat.Vecs[5].Length() > rowIdx {
		allLocs := bat.Vecs[5].GetBytesAt(rowIdx)
		loc := objectio.Location(allLocs)
		log.Printf("    [5] %s: %s (len=%d)\n", CheckpointAttr_AllLocations, loc.String(), len(allLocs))
	}

	// Column 6: checkpoint_lsn (uint64)
	if len(bat.Vecs) > 6 && bat.Vecs[6].Length() > rowIdx {
		ckpLSN := types.DecodeFixed[uint64](bat.Vecs[6].GetRawBytesAt(rowIdx))
		log.Printf("    [6] %s: %d\n", CheckpointAttr_CheckpointLSN, ckpLSN)
	}

	// Column 7: truncate_lsn (uint64)
	if len(bat.Vecs) > 7 && bat.Vecs[7].Length() > rowIdx {
		truncateLSN := types.DecodeFixed[uint64](bat.Vecs[7].GetRawBytesAt(rowIdx))
		log.Printf("    [7] %s: %d\n", CheckpointAttr_TruncateLSN, truncateLSN)
	}

	// Column 8: type (int8)
	if len(bat.Vecs) > 8 && bat.Vecs[8].Length() > rowIdx {
		typ := types.DecodeFixed[int8](bat.Vecs[8].GetRawBytesAt(rowIdx))
		typeName := getTypeName(typ)
		log.Printf("    [8] %s: %d (%s)\n", CheckpointAttr_Type, typ, typeName)
	}
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

func createFileService(ctx context.Context, dir string, useS3Mode bool) (fileservice.FileService, error) {
	if useS3Mode {
		// Use S3FS with DISK endpoint to skip checksum validation
		args := fileservice.ObjectStorageArguments{
			Name:               "SHARED",
			Endpoint:           "DISK",
			Bucket:             dir,
			NoBucketValidation: true,
		}
		fs, err := fileservice.NewS3FS(ctx, args, fileservice.DisabledCacheConfig, nil, true, true)
		if err != nil {
			return nil, fmt.Errorf("failed to create S3 fs (DISK mode): %w", err)
		}
		return fs, nil
	}
	// Use LocalFS with checksum validation
	fs, err := fileservice.NewLocalFS(ctx, "local", dir, fileservice.DisabledCacheConfig, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create local fs: %w", err)
	}
	return fs, nil
}

func deleteRowByEndTS(inputPath string, targetEndTS string, outputPath string, useS3Mode bool) error {
	ctx := context.Background()
	mp, err := mpool.NewMPool("ckp-tool", 0, mpool.NoFixed)
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

	log.Printf("Input file: %s\n", absInputPath)
	log.Printf("Output file: %s\n", absOutputPath)
	log.Printf("Matching end_ts: %s\n", targetEndTS)
	log.Println(strings.Repeat("=", 80))

	// Read the file
	readFs, err := createFileService(ctx, inputDir, useS3Mode)
	if err != nil {
		return fmt.Errorf("failed to create file service for reading: %w", err)
	}
	defer readFs.Close(ctx)

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

	log.Printf("Original row count: %d\n", totalRows)

	// Find the row to delete by matching end_ts
	rowToDelete := findRowByEndTS(srcBat, targetEndTS)
	if rowToDelete < 0 {
		return fmt.Errorf("no row found with end_ts = %s", targetEndTS)
	}

	log.Printf("Found matching row at index %d with end_ts: %s\n", rowToDelete, targetEndTS)

	// Create a new batch without the deleted row
	newBat, err := createBatchWithoutRow(srcBat, rowToDelete, mp)
	if err != nil {
		return err
	}

	log.Printf("New row count: %d\n", newBat.RowCount())

	// Write the new file
	writeFs, err := createFileService(ctx, outputDir, useS3Mode)
	if err != nil {
		return fmt.Errorf("failed to create file service for writing: %w", err)
	}
	defer writeFs.Close(ctx)

	// Delete existing file if overwriting
	if absInputPath == absOutputPath {
		if err := writeFs.Delete(ctx, outputFileName); err != nil {
			log.Printf("Warning: failed to delete existing file: %v\n", err)
		}
	}

	if err := writeBatchToFile(ctx, writeFs, outputFileName, newBat); err != nil {
		return err
	}

	log.Printf("Successfully deleted row with end_ts=%s and wrote to %s\n", targetEndTS, absOutputPath)
	return nil
}

func deleteRowFromCkpFile(inputPath string, rowToDelete int, outputPath string, useS3Mode bool) error {
	ctx := context.Background()
	mp, err := mpool.NewMPool("ckp-tool", 0, mpool.NoFixed)
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

	log.Printf("Input file: %s\n", absInputPath)
	log.Printf("Output file: %s\n", absOutputPath)
	log.Printf("Deleting row: %d\n", rowToDelete)
	log.Println(strings.Repeat("=", 80))

	// Read the file
	readFs, err := createFileService(ctx, inputDir, useS3Mode)
	if err != nil {
		return fmt.Errorf("failed to create file service for reading: %w", err)
	}
	defer readFs.Close(ctx)

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

	log.Printf("Original row count: %d\n", totalRows)

	if rowToDelete < 0 || rowToDelete >= totalRows {
		return fmt.Errorf("row index %d out of range [0, %d)", rowToDelete, totalRows)
	}

	// Create a new batch without the deleted row
	newBat, err := createBatchWithoutRow(srcBat, rowToDelete, mp)
	if err != nil {
		return err
	}

	log.Printf("New row count: %d\n", newBat.RowCount())

	// Write the new file
	writeFs, err := createFileService(ctx, outputDir, useS3Mode)
	if err != nil {
		return fmt.Errorf("failed to create file service for writing: %w", err)
	}
	defer writeFs.Close(ctx)

	// Delete existing file if overwriting
	if absInputPath == absOutputPath {
		if err := writeFs.Delete(ctx, outputFileName); err != nil {
			log.Printf("Warning: failed to delete existing file: %v\n", err)
		}
	}

	if err := writeBatchToFile(ctx, writeFs, outputFileName, newBat); err != nil {
		return err
	}

	log.Printf("Successfully deleted row %d and wrote to %s\n", rowToDelete, absOutputPath)
	return nil
}

// findRowByEndTS finds the row index matching the target end_ts value
func findRowByEndTS(bat *batch.Batch, targetEndTS string) int {
	totalRows := bat.RowCount()
	for rowIdx := 0; rowIdx < totalRows; rowIdx++ {
		if len(bat.Vecs) > 1 && bat.Vecs[1].Length() > rowIdx {
			endTS := types.DecodeFixed[types.TS](bat.Vecs[1].GetRawBytesAt(rowIdx))
			if endTS.ToString() == targetEndTS {
				return rowIdx
			}
		}
	}
	return -1
}

// createBatchWithoutRow creates a new batch without the specified row
func createBatchWithoutRow(srcBat *batch.Batch, rowToDelete int, mp *mpool.MPool) (*batch.Batch, error) {
	totalRows := srcBat.RowCount()
	newBat := batch.NewWithSize(len(srcBat.Vecs))

	// Copy vectors, skipping the deleted row
	for colIdx := 0; colIdx < len(srcBat.Vecs); colIdx++ {
		srcVec := srcBat.Vecs[colIdx]
		newVec := vector.NewVec(*srcVec.GetType())

		for rowIdx := 0; rowIdx < totalRows; rowIdx++ {
			if rowIdx == rowToDelete {
				continue
			}
			if err := newVec.UnionOne(srcVec, int64(rowIdx), mp); err != nil {
				return nil, fmt.Errorf("failed to copy row %d col %d: %w", rowIdx, colIdx, err)
			}
		}
		newBat.Vecs[colIdx] = newVec
	}
	newBat.SetRowCount(totalRows - 1)
	return newBat, nil
}

// writeBatchToFile writes a batch to a checkpoint file
func writeBatchToFile(ctx context.Context, fs fileservice.FileService, fileName string, bat *batch.Batch) error {
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, fileName, fs)
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}

	if _, err = writer.Write(bat); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	if _, err = writer.WriteEnd(ctx); err != nil {
		return fmt.Errorf("failed to finalize write: %w", err)
	}

	return nil
}
