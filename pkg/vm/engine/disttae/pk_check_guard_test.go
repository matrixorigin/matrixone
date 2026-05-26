// Copyright 2024 Matrix Origin
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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testPressureProvider struct {
	level rscthrottler.MemoryPressureLevel
}

func (p *testPressureProvider) MemoryPressure() rscthrottler.MemoryPressureLevel {
	return p.level
}

func TestShouldBailoutOnChangedObjects(t *testing.T) {
	assert.False(t, shouldBailoutOnChangedObjects(0))
	assert.False(t, shouldBailoutOnChangedObjects(maxChangedObjectsForIO))
	assert.True(t, shouldBailoutOnChangedObjects(maxChangedObjectsForIO+1))
	assert.True(t, shouldBailoutOnChangedObjects(1000))
}

func TestShouldBailoutOnCandidateBlocks(t *testing.T) {
	assert.False(t, shouldBailoutOnCandidateBlocks(0))
	assert.False(t, shouldBailoutOnCandidateBlocks(maxCandidateBlksForIO))
	assert.True(t, shouldBailoutOnCandidateBlocks(maxCandidateBlksForIO+1))
	assert.True(t, shouldBailoutOnCandidateBlocks(1000))
}

func TestPKCheckGuardConfigHardening(t *testing.T) {
	cfg := normalizePKCheckGuardConfig(PKCheckGuardConfig{
		Mode:        "bad",
		Concurrency: -1,
	})
	assert.Equal(t, PKCheckGuardModePressure, cfg.Mode)
	assert.GreaterOrEqual(t, cfg.Concurrency, minPKCheckGuardAutoConcurrency)
	assert.LessOrEqual(t, cfg.Concurrency, maxPKCheckGuardAutoConcurrency)

	cfg = normalizePKCheckGuardConfig(PKCheckGuardConfig{
		Mode:        " OFF ",
		Concurrency: maxPKCheckGuardConfiguredConcurrency + 1,
	})
	assert.Equal(t, PKCheckGuardModeOff, cfg.Mode)
	assert.Equal(t, maxPKCheckGuardConfiguredConcurrency, cfg.Concurrency)
}

func TestPKCheckGuardPressureModeSkipsAcquireWithoutPressure(t *testing.T) {
	provider := &testPressureProvider{level: rscthrottler.MemoryPressureNormal}
	guard := newPKCheckGuard(PKCheckGuardConfig{
		Mode:        PKCheckGuardModePressure,
		Concurrency: 1,
	}, provider)

	guard.sem <- struct{}{}
	release, ok, err := acquirePKCheckGuard(context.Background(), guard)
	require.NoError(t, err)
	require.True(t, ok)
	require.Nil(t, release)
}

func TestPKCheckGuardPressureModeSaturatedBailsOut(t *testing.T) {
	provider := &testPressureProvider{level: rscthrottler.MemoryPressureSoft}
	guard := newPKCheckGuard(PKCheckGuardConfig{
		Mode:        PKCheckGuardModePressure,
		Concurrency: 1,
	}, provider)

	guard.sem <- struct{}{}
	release, ok, err := acquirePKCheckGuard(context.Background(), guard)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, release)
}

func TestPKCheckGuardPressureModeAcquiresUnderPressure(t *testing.T) {
	provider := &testPressureProvider{level: rscthrottler.MemoryPressureHard}
	guard := newPKCheckGuard(PKCheckGuardConfig{
		Mode:        PKCheckGuardModePressure,
		Concurrency: 1,
	}, provider)

	release, ok, err := acquirePKCheckGuard(context.Background(), guard)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, release)
	assert.Len(t, guard.sem, 1)
	release()
	assert.Len(t, guard.sem, 0)
}

func TestPKCheckGuardRespectsContextCancellation(t *testing.T) {
	provider := &testPressureProvider{level: rscthrottler.MemoryPressureSoft}
	guard := newPKCheckGuard(PKCheckGuardConfig{
		Mode:        PKCheckGuardModePressure,
		Concurrency: 1,
	}, provider)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	release, ok, err := acquirePKCheckGuard(ctx, guard)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, ok)
	require.Nil(t, release)
}
