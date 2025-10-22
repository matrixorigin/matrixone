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
	"testing"
	"time"
)

// TestDiskMonitorSuite - 磁盘监控测试套件
// 这个测试套件应该在所有其他测试之前运行，用于监控整个测试过程的磁盘使用情况
func TestDiskMonitorSuite(t *testing.T) {
	t.Log("🚀 开始磁盘监控测试套件")

	// 获取当前工作目录
	workDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	// 1. 测试套件开始时的磁盘状态
	t.Run("TestSuiteStart", func(t *testing.T) {
		t.Log("📊 测试套件开始 - 磁盘使用情况")
		LogDiskUsage(t, "SUITE-START", workDir)
		LogSystemInfo(t, "SUITE-START")

		// 检查是否有足够的磁盘空间开始测试
		if !CheckDiskSpace(t, workDir, 2) {
			t.Log("⚠️  警告: 磁盘空间不足，建议清理后再运行测试")
		}
	})

	// 2. 模拟测试运行过程中的磁盘监控
	t.Run("TestSuiteMid", func(t *testing.T) {
		t.Log("📊 测试套件中期 - 磁盘使用情况")

		// 模拟一些文件操作
		tempFile, err := os.CreateTemp("", "matrixone_suite_test_*")
		if err != nil {
			t.Logf("Failed to create temp file: %v", err)
		} else {
			defer os.Remove(tempFile.Name())

			// 写入一些测试数据
			data := make([]byte, 5*1024*1024) // 5MB
			for i := range data {
				data[i] = byte(i % 256)
			}

			_, err = tempFile.Write(data)
			if err != nil {
				t.Logf("Failed to write to temp file: %v", err)
			}
			tempFile.Close()

			// 监控写入后的磁盘使用情况
			LogDiskUsage(t, "SUITE-MID-AFTER-WRITE", workDir)
		}

		// 监控系统资源使用情况
		LogSystemInfo(t, "SUITE-MID")
	})

	// 3. 测试套件结束时的磁盘状态
	t.Run("TestSuiteEnd", func(t *testing.T) {
		t.Log("📊 测试套件结束 - 磁盘使用情况")
		LogDiskUsage(t, "SUITE-END", workDir)
		LogSystemInfo(t, "SUITE-END")

		// 检查磁盘使用趋势
		usage, err := GetDiskUsage(workDir)
		if err == nil {
			usedPercent := float64(usage.Used) / float64(usage.Total) * 100
			if usedPercent > 90 {
				t.Log("🔴 磁盘使用率过高 (>90%)，建议立即清理")
			} else if usedPercent > 80 {
				t.Log("🟡 磁盘使用率较高 (>80%)，建议考虑清理")
			} else if usedPercent > 70 {
				t.Log("🟢 磁盘使用率正常 (>70%)，继续监控")
			} else {
				t.Log("🟢 磁盘使用率良好 (<70%)")
			}
		}
	})

	t.Log("✅ 磁盘监控测试套件完成")
}

// TestDiskMonitorContinuous - 连续磁盘监控测试
// 这个测试用于监控长时间运行的测试过程中的磁盘使用情况
func TestDiskMonitorContinuous(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping continuous disk monitoring in short mode")
	}

	t.Log("🔄 开始连续磁盘监控测试")

	workDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	// 连续监控10次，每次间隔2秒
	for i := 0; i < 10; i++ {
		LogDiskUsage(t, fmt.Sprintf("CONTINUOUS-%d", i+1), workDir)

		// 每5次检查一次系统信息
		if (i+1)%5 == 0 {
			LogSystemInfo(t, fmt.Sprintf("CONTINUOUS-SYS-%d", i+1))
		}

		if i < 9 { // 最后一次不需要等待
			time.Sleep(2 * time.Second)
		}
	}

	t.Log("✅ 连续磁盘监控测试完成")
}

// TestDiskMonitorStress - 磁盘监控压力测试
// 这个测试用于在磁盘压力下监控磁盘使用情况
func TestDiskMonitorStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping disk stress monitoring in short mode")
	}

	t.Log("💪 开始磁盘监控压力测试")

	workDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	// 初始状态
	LogDiskUsage(t, "STRESS-INIT", workDir)

	// 创建多个临时文件来模拟磁盘使用
	var tempFiles []string
	for i := 0; i < 10; i++ {
		tempFile, err := os.CreateTemp("", "matrixone_stress_test_*")
		if err != nil {
			t.Logf("Failed to create temp file %d: %v", i, err)
			continue
		}
		tempFiles = append(tempFiles, tempFile.Name())

		// 写入10MB数据
		data := make([]byte, 10*1024*1024)
		for j := range data {
			data[j] = byte((i + j) % 256)
		}

		_, err = tempFile.Write(data)
		if err != nil {
			t.Logf("Failed to write to temp file %d: %v", i, err)
		}
		tempFile.Close()

		// 每创建2个文件后检查磁盘使用情况
		if (i+1)%2 == 0 {
			LogDiskUsage(t, fmt.Sprintf("STRESS-AFTER-FILE-%d", i+1), workDir)
		}
	}

	// 最终状态
	LogDiskUsage(t, "STRESS-FINAL", workDir)

	// 清理临时文件
	for _, tempFile := range tempFiles {
		os.Remove(tempFile)
	}

	// 清理后状态
	LogDiskUsage(t, "STRESS-AFTER-CLEANUP", workDir)

	t.Log("✅ 磁盘监控压力测试完成")
}

// TestDiskMonitorCleanup - 磁盘清理建议测试
func TestDiskMonitorCleanup(t *testing.T) {
	t.Log("🧹 开始磁盘清理建议测试")

	workDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	usage, err := GetDiskUsage(workDir)
	if err != nil {
		t.Logf("Failed to get disk usage: %v", err)
		return
	}

	usedPercent := float64(usage.Used) / float64(usage.Total) * 100

	t.Logf("当前磁盘使用率: %.2f%%", usedPercent)

	if usedPercent > 90 {
		t.Log("🔴 磁盘使用率过高，建议立即执行以下清理操作:")
		t.Log("  - 清理临时文件: rm -rf /tmp/matrixone_*")
		t.Log("  - 清理测试日志: find . -name '*.log' -mtime +7 -delete")
		t.Log("  - 清理构建缓存: go clean -cache")
		t.Log("  - 清理测试数据: find . -name 'testdata' -type d -exec rm -rf {} +")
	} else if usedPercent > 80 {
		t.Log("🟡 磁盘使用率较高，建议考虑以下清理操作:")
		t.Log("  - 清理旧的测试文件")
		t.Log("  - 清理构建缓存")
		t.Log("  - 清理日志文件")
	} else {
		t.Log("🟢 磁盘使用率正常，无需立即清理")
	}

	t.Log("✅ 磁盘清理建议测试完成")
}
