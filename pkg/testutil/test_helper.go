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
	"os"
	"testing"
)

// TestWithDiskMonitoring 是一个测试包装器，自动在测试前后监控磁盘使用情况
func TestWithDiskMonitoring(t *testing.T, testName string, testFunc func(t *testing.T)) {
	t.Run(testName, func(t *testing.T) {
		// 测试前监控
		workDir, _ := os.Getwd()
		LogDiskUsage(t, testName+"-BEFORE", workDir)

		// 执行实际测试
		testFunc(t)

		// 测试后监控
		LogDiskUsage(t, testName+"-AFTER", workDir)
	})
}

// SetupTestWithDiskCheck 在测试设置时检查磁盘空间
func SetupTestWithDiskCheck(t *testing.T, minFreeGB uint64) bool {
	workDir, err := os.Getwd()
	if err != nil {
		t.Logf("Failed to get working directory: %v", err)
		return false
	}

	LogDiskUsage(t, "SETUP", workDir)
	return CheckDiskSpace(t, workDir, minFreeGB)
}

// TeardownTestWithDiskCheck 在测试清理时检查磁盘使用情况
func TeardownTestWithDiskCheck(t *testing.T) {
	workDir, err := os.Getwd()
	if err != nil {
		t.Logf("Failed to get working directory: %v", err)
		return
	}

	LogDiskUsage(t, "TEARDOWN", workDir)
	LogSystemInfo(t, "TEARDOWN")
}

// MonitorDiskDuringTest 在测试过程中监控磁盘使用情况
func MonitorDiskDuringTest(t *testing.T, stage string) {
	workDir, err := os.Getwd()
	if err != nil {
		t.Logf("Failed to get working directory: %v", err)
		return
	}

	LogDiskUsage(t, stage, workDir)
}

// CheckDiskSpaceBeforeTest 在测试前检查磁盘空间是否足够
func CheckDiskSpaceBeforeTest(t *testing.T, minFreeGB uint64) bool {
	workDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	hasSpace := CheckDiskSpace(t, workDir, minFreeGB)
	if !hasSpace {
		t.Logf("⚠️  警告: 磁盘空间不足，测试可能会失败或运行缓慢")
	}

	return hasSpace
}

// LogTestProgress 记录测试进度和磁盘使用情况
func LogTestProgress(t *testing.T, progress string) {
	workDir, err := os.Getwd()
	if err != nil {
		t.Logf("Failed to get working directory: %v", err)
		return
	}

	t.Logf("📊 测试进度: %s", progress)
	LogDiskUsage(t, "PROGRESS-"+progress, workDir)
}
