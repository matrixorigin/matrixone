// Copyright 2023 Matrix Origin
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

package testutil

import (
	"fmt"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// DiskUsage represents disk usage information
type DiskUsage struct {
	Total     uint64
	Free      uint64
	Used      uint64
	Available uint64
	Path      string
	Timestamp time.Time
}

// GetDiskUsage returns disk usage information for the given path
func GetDiskUsage(path string) (*DiskUsage, error) {
	var stat syscall.Statfs_t
	err := syscall.Statfs(path, &stat)
	if err != nil {
		return nil, err
	}

	// Calculate sizes
	total := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bavail * uint64(stat.Bsize)
	used := total - free

	return &DiskUsage{
		Total:     total,
		Free:      free,
		Used:      used,
		Available: free,
		Path:      path,
		Timestamp: time.Now(),
	}, nil
}

// FormatBytes converts bytes to human readable format
func FormatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// LogDiskUsage logs disk usage information to testing.T
func LogDiskUsage(t *testing.T, stage string, path string) {
	usage, err := GetDiskUsage(path)
	if err != nil {
		t.Logf("[%s] Failed to get disk usage for %s: %v", stage, path, err)
		return
	}

	usedPercent := float64(usage.Used) / float64(usage.Total) * 100
	freePercent := float64(usage.Free) / float64(usage.Total) * 100

	t.Logf("[%s] Disk Usage for %s:", stage, path)
	t.Logf("  Total: %s", FormatBytes(usage.Total))
	t.Logf("  Used:  %s (%.2f%%)", FormatBytes(usage.Used), usedPercent)
	t.Logf("  Free:  %s (%.2f%%)", FormatBytes(usage.Free), freePercent)
	t.Logf("  Available: %s", FormatBytes(usage.Available))
	t.Logf("  Timestamp: %s", usage.Timestamp.Format(time.RFC3339))

	// Warning if disk usage is high
	if usedPercent > 90 {
		t.Logf("  ⚠️  WARNING: Disk usage is above 90%%!")
	} else if usedPercent > 80 {
		t.Logf("  ⚠️  WARNING: Disk usage is above 80%%!")
	}
}

// LogSystemInfo logs general system information
func LogSystemInfo(t *testing.T, stage string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	t.Logf("[%s] System Information:", stage)
	t.Logf("  Go Version: %s", runtime.Version())
	t.Logf("  OS: %s", runtime.GOOS)
	t.Logf("  Architecture: %s", runtime.GOARCH)
	t.Logf("  NumCPU: %d", runtime.NumCPU())
	t.Logf("  Goroutines: %d", runtime.NumGoroutine())
	t.Logf("  Memory Allocated: %s", FormatBytes(m.Alloc))
	t.Logf("  Memory Total Allocated: %s", FormatBytes(m.TotalAlloc))
	t.Logf("  Memory System: %s", FormatBytes(m.Sys))
	t.Logf("  GC Cycles: %d", m.NumGC)
}

// CheckDiskSpace checks if there's enough disk space and logs warning if not
func CheckDiskSpace(t *testing.T, path string, minFreeGB uint64) bool {
	usage, err := GetDiskUsage(path)
	if err != nil {
		t.Logf("Failed to check disk space: %v", err)
		return false
	}

	minFreeBytes := minFreeGB * 1024 * 1024 * 1024
	hasEnoughSpace := usage.Free >= minFreeBytes

	if !hasEnoughSpace {
		t.Logf("❌ INSUFFICIENT DISK SPACE!")
		t.Logf("  Required: %d GB", minFreeGB)
		t.Logf("  Available: %s", FormatBytes(usage.Free))
		t.Logf("  Path: %s", path)
	} else {
		t.Logf("✅ Sufficient disk space available: %s", FormatBytes(usage.Free))
	}

	return hasEnoughSpace
}

// MonitorDiskUsageDuringTest monitors disk usage during test execution
func MonitorDiskUsageDuringTest(t *testing.T, testName string, path string) {
	// Initial disk usage
	LogDiskUsage(t, fmt.Sprintf("%s-INIT", testName), path)
	LogSystemInfo(t, fmt.Sprintf("%s-INIT", testName))

	// Check if we have enough space to start
	CheckDiskSpace(t, path, 1) // Require at least 1GB free space
}
