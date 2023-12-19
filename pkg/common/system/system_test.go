// Copyright 2021 - 2022 Matrix Origin
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

package system

import (
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/stretchr/testify/require"
)

func TestCPU(t *testing.T) {
	defer leaktest.AfterTest(t)()
	st := stopper.NewStopper("test")
	defer st.Stop()
	Run(st)
	mcpu := NumCPU()
	time.Sleep(2 * time.Second)
	acpu := CPUAvailable()
	require.Equal(t, true, float64(mcpu) >= acpu)
}

func TestMemory(t *testing.T) {
	totalMemory := MemoryTotal()
	availableMemory := MemoryAvailable()
	require.Equal(t, true, totalMemory >= availableMemory)
}
