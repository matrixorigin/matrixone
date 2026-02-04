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
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

// SyncProtectionRequest represents a sync protection request
type SyncProtectionRequest struct {
	JobID      string `json:"job_id"`
	BF         string `json:"bf"` // Base64 encoded BloomFilter
	ValidTS    int64  `json:"valid_ts"`
	TestObject string `json:"test_object"` // Test object name (for debugging)
}

// SyncProtectionTester tests the sync protection mechanism
type SyncProtectionTester struct {
	db             *sql.DB
	dataDir        string
	jobID          string
	protectedFiles []string
	sampleCount    int
	verbose        bool
	waitTime       int
}

func NewSyncProtectionTester(dsn, dataDir string, sampleCount int, verbose bool, waitTime int) (*SyncProtectionTester, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("failed to connect to database: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("failed to ping database: %v", err)
	}

	return &SyncProtectionTester{
		db:          db,
		dataDir:     dataDir,
		jobID:       fmt.Sprintf("sync-test-%d", time.Now().UnixNano()),
		sampleCount: sampleCount,
		verbose:     verbose,
		waitTime:    waitTime,
	}, nil
}

func (t *SyncProtectionTester) Close() {
	if t.db != nil {
		t.db.Close()
	}
}

// ScanObjectFiles scans the directory for object files
func (t *SyncProtectionTester) ScanObjectFiles() ([]string, error) {
	var objects []string

	err := filepath.Walk(t.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		// Match object file name pattern
		name := info.Name()
		// MatrixOne object files are typically UUID format with underscore
		// Format: 019c226d-9e98-7ecc-9662-712ff0edcbfb_00000 (42 characters)
		if len(name) == 42 && strings.Contains(name, "_") && strings.Count(name, "-") == 4 {
			objects = append(objects, name)
		}
		return nil
	})

	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("failed to scan directory: %v", err)
	}

	return objects, nil
}

// SelectRandomObjects randomly selects objects
func (t *SyncProtectionTester) SelectRandomObjects(objects []string, count int) []string {
	if len(objects) <= count {
		return objects
	}

	// Copy slice to avoid modifying original data
	copied := make([]string, len(objects))
	copy(copied, objects)

	// Shuffle randomly
	rand.Shuffle(len(copied), func(i, j int) {
		copied[i], copied[j] = copied[j], copied[i]
	})

	return copied[:count]
}

// BuildBloomFilter builds a BloomFilter using xorfilter (deterministic hash)
func (t *SyncProtectionTester) BuildBloomFilter(objects []string) (string, error) {
	// Create a containers.Vector with all object names
	vec := containers.MakeVector(types.T_varchar.ToType(), mpool.MustNewZero())
	defer vec.Close()

	for _, obj := range objects {
		vec.Append([]byte(obj), false)
	}

	// Create BloomFilter using index.NewBloomFilter (xorfilter based)
	bf, err := index.NewBloomFilter(vec)
	if err != nil {
		return "", moerr.NewInternalErrorNoCtxf("failed to create BloomFilter: %v", err)
	}

	// Marshal BloomFilter
	data, err := bf.Marshal()
	if err != nil {
		return "", moerr.NewInternalErrorNoCtxf("failed to marshal BloomFilter: %v", err)
	}

	// Base64 encode
	base64Data := base64.StdEncoding.EncodeToString(data)

	if t.verbose {
		fmt.Printf("  BloomFilter: %d objects, %d bytes, base64 len=%d\n", len(objects), len(data), len(base64Data))
	}

	return base64Data, nil
}

// RegisterProtection registers protection
func (t *SyncProtectionTester) RegisterProtection(objects []string) error {
	// Build BloomFilter
	bfData, err := t.BuildBloomFilter(objects)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to build BloomFilter: %v", err)
	}

	// Send first protected object name for testing
	testObject := ""
	if len(objects) > 0 {
		testObject = objects[0]
	}

	req := SyncProtectionRequest{
		JobID:      t.jobID,
		BF:         bfData,
		ValidTS:    time.Now().UnixNano(),
		TestObject: testObject,
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to marshal request: %v", err)
	}

	query := fmt.Sprintf("SELECT mo_ctl('dn', 'diskcleaner', 'register_sync_protection.%s')", string(jsonData))

	if t.verbose {
		fmt.Printf("  SQL length: %d\n", len(query))
	}

	var result string
	err = t.db.QueryRow(query).Scan(&result)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to register protection: %v", err)
	}

	if t.verbose {
		fmt.Printf("  Result: %s\n", result)
	}

	// Check if successful
	if strings.Contains(strings.ToLower(result), "error") {
		return moerr.NewInternalErrorNoCtxf("register protection returned error: %s", result)
	}

	t.protectedFiles = objects
	return nil
}

// RenewProtection renews protection
func (t *SyncProtectionTester) RenewProtection() error {
	req := SyncProtectionRequest{
		JobID:   t.jobID,
		ValidTS: time.Now().UnixNano(),
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to marshal request: %v", err)
	}

	query := fmt.Sprintf("SELECT mo_ctl('dn', 'diskcleaner', 'renew_sync_protection.%s')", string(jsonData))

	if t.verbose {
		fmt.Printf("  SQL: %s\n", query)
	}

	var result string
	err = t.db.QueryRow(query).Scan(&result)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to renew protection: %v", err)
	}

	if t.verbose {
		fmt.Printf("  Result: %s\n", result)
	}

	return nil
}

// UnregisterProtection unregisters protection
func (t *SyncProtectionTester) UnregisterProtection() error {
	req := SyncProtectionRequest{
		JobID: t.jobID,
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to marshal request: %v", err)
	}

	query := fmt.Sprintf("SELECT mo_ctl('dn', 'diskcleaner', 'unregister_sync_protection.%s')", string(jsonData))

	if t.verbose {
		fmt.Printf("  SQL: %s\n", query)
	}

	var result string
	err = t.db.QueryRow(query).Scan(&result)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to unregister protection: %v", err)
	}

	if t.verbose {
		fmt.Printf("  Result: %s\n", result)
	}

	return nil
}

// TriggerGC triggers GC
func (t *SyncProtectionTester) TriggerGC() error {
	query := "SELECT mo_ctl('dn', 'diskcleaner', 'force_gc')"

	if t.verbose {
		fmt.Printf("  SQL: %s\n", query)
	}

	var result string
	err := t.db.QueryRow(query).Scan(&result)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to trigger GC: %v", err)
	}

	if t.verbose {
		fmt.Printf("  Result: %s\n", result)
	}

	return nil
}

// CheckFilesExist checks if files exist
func (t *SyncProtectionTester) CheckFilesExist() (existing, deleted []string) {
	for _, file := range t.protectedFiles {
		// Search for file in data directory
		found := false
		filepath.Walk(t.dataDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if info.Name() == file {
				found = true
				return filepath.SkipAll
			}
			return nil
		})
		if found {
			existing = append(existing, file)
		} else {
			deleted = append(deleted, file)
		}
	}
	return
}

// RunTest runs the test
func (t *SyncProtectionTester) RunTest() error {
	fmt.Println("========================================")
	fmt.Println("Sync Protection Test (BloomFilter)")
	fmt.Println("========================================")
	fmt.Printf("Job ID: %s\n", t.jobID)
	fmt.Printf("Data directory: %s\n", t.dataDir)
	fmt.Printf("Sample count: %d\n", t.sampleCount)
	fmt.Printf("Wait time: %d seconds\n", t.waitTime)
	fmt.Println()

	// Ensure cleanup on exit
	registered := false
	defer func() {
		if registered {
			fmt.Println("[Cleanup] Ensuring protection is unregistered...")
			if err := t.UnregisterProtection(); err != nil {
				// Ignore error if already unregistered
				fmt.Printf("  (Already unregistered or error: %v)\n", err)
			} else {
				fmt.Println("  ✓ Cleanup successful!")
			}
		}
	}()

	// Step 1: Scan object files
	fmt.Println("[Step 1] Scanning object files...")
	objects, err := t.ScanObjectFiles()
	if err != nil {
		return err
	}
	fmt.Printf("  Found %d object files\n", len(objects))

	if len(objects) == 0 {
		return moerr.NewInternalErrorNoCtxf("no object files found, please check data directory: %s", t.dataDir)
	}

	// Step 2: Randomly select objects
	fmt.Println("[Step 2] Randomly selecting objects...")
	selected := t.SelectRandomObjects(objects, t.sampleCount)
	fmt.Printf("  Selected %d objects:\n", len(selected))
	for i, obj := range selected {
		if i < 5 {
			fmt.Printf("    - %s\n", obj)
		} else if i == 5 {
			fmt.Printf("    - ... (%d more)\n", len(selected)-5)
			break
		}
	}

	// Step 3: Build BloomFilter and register protection
	fmt.Println("[Step 3] Building BloomFilter and registering sync protection...")
	if err := t.RegisterProtection(selected); err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to register protection: %v", err)
	}
	registered = true
	fmt.Println("  ✓ Registration successful!")

	// Step 4: Check initial file status
	fmt.Println("[Step 4] Checking initial file status...")
	existingBefore, deletedBefore := t.CheckFilesExist()
	fmt.Printf("  Existing: %d, Deleted: %d\n", len(existingBefore), len(deletedBefore))

	// Step 5: Trigger GC
	fmt.Println("[Step 5] Triggering GC...")
	if err := t.TriggerGC(); err != nil {
		fmt.Printf("  ⚠ Warning: Failed to trigger GC: %v\n", err)
	} else {
		fmt.Println("  ✓ GC triggered successfully!")
	}

	// Wait for GC to complete
	fmt.Printf("[Step 6] Waiting for GC to complete (%d seconds)...\n", t.waitTime)
	time.Sleep(time.Duration(t.waitTime) * time.Second)

	// Step 7: Check file protection status
	fmt.Println("[Step 7] Checking file protection status...")
	existingAfter, deletedAfter := t.CheckFilesExist()
	fmt.Printf("  Existing: %d, Deleted: %d\n", len(existingAfter), len(deletedAfter))

	// Compare results
	newlyDeleted := len(deletedAfter) - len(deletedBefore)
	if newlyDeleted > 0 {
		fmt.Printf("  ✗ [FAILED] %d protected files were deleted!\n", newlyDeleted)
		for _, f := range deletedAfter {
			found := false
			for _, bf := range deletedBefore {
				if f == bf {
					found = true
					break
				}
			}
			if !found {
				fmt.Printf("    - Deleted: %s\n", f)
			}
		}
		// Validation failed, stop test
		return moerr.NewInternalErrorNoCtxf("protection mechanism validation failed: %d protected files were deleted", newlyDeleted)
	} else {
		fmt.Println("  ✓ [SUCCESS] All protected files were not deleted!")
	}

	// Step 8: Test renewal
	fmt.Println("[Step 8] Testing renewal...")
	if err := t.RenewProtection(); err != nil {
		fmt.Printf("  ⚠ Warning: Renewal failed: %v\n", err)
	} else {
		fmt.Println("  ✓ Renewal successful!")
	}

	// Step 9: Unregister protection
	fmt.Println("[Step 9] Unregistering protection (soft delete)...")
	if err := t.UnregisterProtection(); err != nil {
		fmt.Printf("  ⚠ Warning: Unregister failed: %v\n", err)
	} else {
		registered = false // Mark as unregistered so defer won't try again
		fmt.Println("  ✓ Unregister successful!")
	}

	// Step 10: Trigger GC again
	fmt.Println("[Step 10] Triggering GC again...")
	if err := t.TriggerGC(); err != nil {
		fmt.Printf("  ⚠ Warning: Failed to trigger GC: %v\n", err)
	} else {
		fmt.Println("  ✓ GC triggered successfully!")
	}

	// Wait for GC to complete
	fmt.Printf("[Step 11] Waiting for GC to complete (%d seconds)...\n", t.waitTime)
	time.Sleep(time.Duration(t.waitTime) * time.Second)

	// Step 12: Final check
	fmt.Println("[Step 12] Final check...")
	existingFinal, deletedFinal := t.CheckFilesExist()
	fmt.Printf("  Existing: %d, Deleted: %d\n", len(existingFinal), len(deletedFinal))

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("Test completed!")
	fmt.Println("========================================")

	return nil
}

// PrepareSyncProtectionCommand prepares the sync protection test command
func PrepareSyncProtectionCommand() *cobra.Command {
	var (
		dsn         string
		dataDir     string
		sampleCount int
		verbose     bool
		waitTime    int
	)

	cmd := &cobra.Command{
		Use:   "sync-protection",
		Short: "Test sync protection mechanism",
		Long: `Test cross-cluster sync protection mechanism.

This command will:
1. Scan the specified directory for object files
2. Randomly select some objects to build a BloomFilter
3. Register BloomFilter protection
4. Trigger GC and verify protected files are not deleted
5. Test renewal and unregister functionality`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tester, err := NewSyncProtectionTester(dsn, dataDir, sampleCount, verbose, waitTime)
			if err != nil {
				return err
			}
			defer tester.Close()

			return tester.RunTest()
		},
	}

	cmd.Flags().StringVar(&dsn, "dsn", "root:111@tcp(127.0.0.1:6001)/", "Database connection string")
	cmd.Flags().StringVar(&dataDir, "data-dir", "./mo-data/shared", "Data directory path")
	cmd.Flags().IntVar(&sampleCount, "sample", 10, "Number of objects to sample")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "Show verbose output")
	cmd.Flags().IntVar(&waitTime, "wait", 30, "Time to wait for GC to complete (seconds)")

	return cmd
}
