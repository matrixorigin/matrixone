// Copyright 2026 Matrix Origin
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

package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	gc "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

func TestPreGCBootstrapProtectionExistsBeforeFirstGCOpportunity(t *testing.T) {
	machineReads := 0
	setTestGenerationMachineID(t, func() ([]byte, error) {
		machineReads++
		return nil, errors.New("machine identity unavailable on this platform")
	})
	dir := t.TempDir()
	shard := metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{ShardID: 7, LogShardID: 8},
		ReplicaID:     9,
	}
	readRef := bytes.Repeat([]byte{7}, 32)
	const objectName = "protected-at-open"
	var manager *gc.SyncProtectionManager
	hookCalls := 0
	var observed options.PreGCBootstrapContext
	opts := (&options.Options{
		Shard: shard,
		PreGCBootstrap: func(ctx context.Context, bootstrap options.PreGCBootstrapContext) error {
			hookCalls++
			observed = bootstrap
			protector, ok := bootstrap.Protector.(gc.SidecarReadProtector)
			if !ok || protector.Manager == nil {
				return errors.New("missing pre-GC protection manager")
			}
			manager = protector.Manager
			if manager.IsGCRunning() {
				return errors.New("GC started before bootstrap")
			}
			return protector.Register(ctx, readRef, []string{objectName}, time.Now().Add(time.Hour))
		},
	}).FillDefaults(dir)

	tae, err := Open(context.Background(), dir, opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tae.Close()) })
	require.Equal(t, 1, hookCalls)
	require.Equal(t, shard, observed.Shard)
	require.NotNil(t, observed.SharedFileService)
	require.NotNil(t, manager)
	require.True(t, manager.IsProtected(objectName),
		"replayed protection must be visible when the cleaner gets its first work")
	require.Zero(t, machineReads, "a generic hook must not acquire Sirius directory identity")
	_, err = os.Stat(filepath.Join(dir, storageGenerationFile))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPreGCBootstrapFailurePreventsOpenAndCleanerStart(t *testing.T) {
	setTestGenerationMachineID(t, func() ([]byte, error) { return []byte("test-host"), nil })
	dir := t.TempDir()
	bootstrapErr := errors.New("injected pre-GC bootstrap failure")
	var manager *gc.SyncProtectionManager
	opts := (&options.Options{
		PreGCBootstrap: func(_ context.Context, bootstrap options.PreGCBootstrapContext) error {
			if bootstrap.StorageGeneration == nil {
				return errors.New("missing locked storage generation callback")
			}
			generation, generationErr := bootstrap.StorageGeneration()
			if generationErr != nil {
				return generationErr
			}
			if len(generation) != 32 {
				return errors.New("invalid locked storage generation")
			}
			protector, ok := bootstrap.Protector.(gc.SidecarReadProtector)
			if !ok || protector.Manager == nil {
				return errors.New("missing pre-GC protection manager")
			}
			manager = protector.Manager
			if manager.IsGCRunning() {
				return errors.New("GC started before bootstrap")
			}
			return bootstrapErr
		},
	}).FillDefaults(dir)

	_, err := Open(context.Background(), dir, opts)
	require.ErrorIs(t, err, bootstrapErr)
	require.NotNil(t, manager)
	require.False(t, manager.IsGCRunning())
	guard, guardErr := manager.BeginProtection()
	require.NoError(t, guardErr,
		"a failed hook must return before DiskCleaner.Start can take the GC barrier")
	guard.Close()
	lock, lockErr := createDBLock(dir)
	require.NoError(t, lockErr, "failed open must release the directory after rollback")
	require.NoError(t, lock.Close())
}
