//go:build integration && viewmetadatabench

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend_test

import (
	"errors"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// 单独的 profile 运行才设置此目录；CPU 只覆盖测量循环，排除建库/warm-up。
// heap profile 是进程快照，包含服务背景与 setup，不能充当请求峰值证明。
func startViewMetadataBenchProfile(b *testing.B) func() {
	dir := os.Getenv("MO_VIEW_BENCH_PROFILE_DIR")
	if dir == "" {
		return func() {}
	}
	require.NoError(b, os.MkdirAll(dir, 0700))
	prefix := "CLAUDE_" + strings.ReplaceAll(b.Name(), "/", "_") + "-"
	cpu, err := os.CreateTemp(dir, prefix+"*.cpu.pprof")
	require.NoError(b, err)
	if err = pprof.StartCPUProfile(cpu); err != nil {
		require.NoError(b, errors.Join(err, cpu.Close()))
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			pprof.StopCPUProfile()
			require.NoError(b, cpu.Close())
			b.Logf("measured CPU profile: %s", cpu.Name())
			heap, err := os.CreateTemp(dir, prefix+"*.heap.pprof")
			require.NoError(b, err)
			err = pprof.WriteHeapProfile(heap)
			closeErr := heap.Close()
			require.NoError(b, errors.Join(err, closeErr))
			b.Logf("process heap snapshot (not request peak): %s", heap.Name())
		})
	}
}
