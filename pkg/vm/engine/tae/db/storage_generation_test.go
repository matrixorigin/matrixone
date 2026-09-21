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
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func setTestGenerationMachineID(t *testing.T, read func() ([]byte, error)) {
	t.Helper()
	original := readStorageGenerationMachineID
	readStorageGenerationMachineID = read
	t.Cleanup(func() { readStorageGenerationMachineID = original })
}

func lockedGenerationDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	lock, err := createDBLock(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lock.Close()) })
	return dir
}

func TestStorageGenerationStableRestartAndDirectoryBinding(t *testing.T) {
	machine := []byte("machine-a")
	setTestGenerationMachineID(t, func() ([]byte, error) { return machine, nil })
	firstDir := lockedGenerationDir(t)
	first, err := loadOrCreateStorageGeneration(firstDir)
	require.NoError(t, err)
	restarted, err := loadOrCreateStorageGeneration(firstDir)
	require.NoError(t, err)
	require.Equal(t, first, restarted)
	secondDir := lockedGenerationDir(t)
	token, err := os.ReadFile(filepath.Join(firstDir, storageGenerationFile))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(secondDir, storageGenerationFile), token, 0o600))
	copied, err := loadOrCreateStorageGeneration(secondDir)
	require.NoError(t, err)
	require.NotEqual(t, first, copied, "copied tokens remain bound to the original directory")
	machine = []byte("machine-b")
	crossHost, err := loadOrCreateStorageGeneration(firstDir)
	require.NoError(t, err)
	require.NotEqual(t, first, crossHost)
}

func TestStorageGenerationRejectsMalformedAndOversizedToken(t *testing.T) {
	setTestGenerationMachineID(t, func() ([]byte, error) { return []byte("test-host"), nil })
	for _, size := range []int{0, sha256.Size - 1, sha256.Size + 1} {
		dir := lockedGenerationDir(t)
		payload := bytes.Repeat([]byte{7}, size)
		name := filepath.Join(dir, storageGenerationFile)
		require.NoError(t, os.WriteFile(name, payload, 0o600))
		_, err := loadOrCreateStorageGeneration(dir)
		require.Error(t, err)
		retained, err := os.ReadFile(name)
		require.NoError(t, err)
		require.Equal(t, payload, retained, "invalid authority must not be silently replaced")
	}
}

func TestStorageGenerationRecoversInterruptedPublication(t *testing.T) {
	setTestGenerationMachineID(t, func() ([]byte, error) { return []byte("test-host"), nil })
	dir := lockedGenerationDir(t)
	name := filepath.Join(dir, storageGenerationFile)
	require.NoError(t, os.WriteFile(name+".tmp", []byte("partial"), 0o600))
	_, err := loadOrCreateStorageGeneration(dir)
	require.NoError(t, err)
	token, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Len(t, token, sha256.Size)
	_, err = os.Stat(name + ".tmp")
	require.ErrorIs(t, err, os.ErrNotExist)
	require.ErrorIs(t, publishStorageGeneration(name, bytes.Repeat([]byte{9}, sha256.Size)), os.ErrExist)
	retained, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, token, retained, "publication must never replace an existing generation")
}

func TestStorageGenerationMissingHostIdentityDoesNotCreateToken(t *testing.T) {
	missing := errors.New("machine identity unavailable")
	setTestGenerationMachineID(t, func() ([]byte, error) { return nil, missing })
	dir := lockedGenerationDir(t)
	_, err := loadOrCreateStorageGeneration(dir)
	require.ErrorIs(t, err, missing)
	_, err = os.Stat(filepath.Join(dir, storageGenerationFile))
	require.ErrorIs(t, err, os.ErrNotExist)
}
