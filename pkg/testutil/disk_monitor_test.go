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

// TestDiskMonitorInitial - 测试初期磁盘监控
func TestDiskMonitorInitial(t *testing.T) {
	t.Log("=== 测试初期磁盘使用情况监控 ===")

	// 获取当前工作目录
	workDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	// 监控当前工作目录的磁盘使用情况
	MonitorDiskUsageDuringTest(t, "INITIAL", workDir)

	// 监控临时目录
	tempDir := os.TempDir()
	LogDiskUsage(t, "INITIAL-TEMP", tempDir)

	// 监控用户主目录
	homeDir, err := os.UserHomeDir()
	if err == nil {
		LogDiskUsage(t, "INITIAL-HOME", homeDir)
	}

	// 检查是否有足够的磁盘空间开始测试
	if !CheckDiskSpace(t, workDir, 2) { // 需要至少2GB空间
		t.Log("⚠️  警告: 磁盘空间不足，测试可能会失败")
	}

	t.Log("✅ 测试初期磁盘监控完成")
}

// TestDiskMonitorMid - 测试中期磁盘监控
func TestDiskMonitorMid(t *testing.T) {
	t.Log("=== 测试中期磁盘使用情况监控 ===")

	workDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	// 监控磁盘使用情况
	LogDiskUsage(t, "MID", workDir)
	LogSystemInfo(t, "MID")

	// 检查磁盘空间
	CheckDiskSpace(t, workDir, 1) // 中期检查，至少需要1GB

	// 模拟一些文件操作来测试磁盘使用
	tempFile, err := os.CreateTemp("", "matrixone_test_*")
	if err != nil {
		t.Logf("Failed to create temp file: %v", err)
	} else {
		defer os.Remove(tempFile.Name())

		// 写入一些数据
		data := make([]byte, 1024*1024) // 1MB
		for i := range data {
			data[i] = byte(i % 256)
		}

		_, err = tempFile.Write(data)
		if err != nil {
			t.Logf("Failed to write to temp file: %v", err)
		}
		tempFile.Close()

		// 再次检查磁盘使用情况
		LogDiskUsage(t, "MID-AFTER-WRITE", workDir)
	}

	t.Log("✅ 测试中期磁盘监控完成")
}

// TestDiskMonitorFinal - 测试后期磁盘监控
func TestDiskMonitorFinal(t *testing.T) {
	t.Log("=== 测试后期磁盘使用情况监控 ===")

	workDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	// 最终磁盘使用情况检查
	LogDiskUsage(t, "FINAL", workDir)
	LogSystemInfo(t, "FINAL")

	// 检查磁盘空间
	hasSpace := CheckDiskSpace(t, workDir, 1)

	// 清理建议
	if !hasSpace {
		t.Log("🧹 建议清理以下内容以释放磁盘空间:")
		t.Log("  - 清理临时文件")
		t.Log("  - 清理测试生成的日志文件")
		t.Log("  - 清理构建缓存")
		t.Log("  - 清理旧的测试数据")
	}

	// 显示磁盘使用趋势
	usage, err := GetDiskUsage(workDir)
	if err == nil {
		usedPercent := float64(usage.Used) / float64(usage.Total) * 100
		if usedPercent > 85 {
			t.Log("📊 磁盘使用率较高，建议定期清理")
		} else if usedPercent > 70 {
			t.Log("📊 磁盘使用率中等，注意监控")
		} else {
			t.Log("📊 磁盘使用率正常")
		}
	}

	t.Log("✅ 测试后期磁盘监控完成")
}

// TestDiskMonitorStress - 压力测试磁盘监控
func TestDiskMonitorStress(t *testing.T) {
	t.Log("=== 磁盘监控压力测试 ===")

	workDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	// 初始状态
	LogDiskUsage(t, "STRESS-INIT", workDir)

	// 创建多个临时文件来模拟磁盘使用
	var tempFiles []string
	for i := 0; i < 5; i++ {
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

		// 每创建一个文件后检查磁盘使用情况
		LogDiskUsage(t, fmt.Sprintf("STRESS-AFTER-FILE-%d", i+1), workDir)
	}

	// 清理临时文件
	for _, tempFile := range tempFiles {
		os.Remove(tempFile)
	}

	// 最终状态
	LogDiskUsage(t, "STRESS-FINAL", workDir)

	t.Log("✅ 磁盘监控压力测试完成")
}

// TestDiskMonitorContinuous - 连续监控测试
func TestDiskMonitorContinuous(t *testing.T) {
	t.Log("=== 连续磁盘监控测试 ===")

	workDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	// 连续监控5次，每次间隔1秒
	for i := 0; i < 5; i++ {
		LogDiskUsage(t, fmt.Sprintf("CONTINUOUS-%d", i+1), workDir)

		if i < 4 { // 最后一次不需要等待
			time.Sleep(1 * time.Second)
		}
	}

	t.Log("✅ 连续磁盘监控测试完成")
}
