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
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const storageGenerationFile = "SIRIUS-GENERATION"

var readStorageGenerationMachineID = func() ([]byte, error) {
	value, err := readSmallGenerationFile("/etc/machine-id", 128)
	if err != nil {
		return nil, err
	}
	value = []byte(strings.TrimSpace(string(value)))
	if len(value) == 0 {
		return nil, moerr.NewInvalidStateNoCtx("empty TAE storage generation machine identity")
	}
	return value, nil
}

func readSmallGenerationFile(name string, maximum int64) ([]byte, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	value, readErr := io.ReadAll(io.LimitReader(f, maximum+1))
	if err := errors.Join(readErr, f.Close()); err != nil {
		return nil, err
	}
	if int64(len(value)) > maximum {
		return nil, moerr.NewInvalidStateNoCtxf("TAE generation record exceeds %d bytes", maximum)
	}
	return value, nil
}

// loadOrCreateStorageGeneration may run only while createDBLock owns dirname.
// It identifies one same-host persistent directory; it does not prove exclusive
// ownership of a remote/shared journal namespace.
func loadOrCreateStorageGeneration(dirname string) ([]byte, error) {
	canonical, err := filepath.Abs(dirname)
	if err != nil {
		return nil, err
	}
	canonical, err = filepath.EvalSymlinks(canonical)
	if err != nil {
		return nil, err
	}
	machineID, err := readStorageGenerationMachineID()
	if err != nil {
		return nil, err
	}
	tokenPath := filepath.Join(canonical, storageGenerationFile)
	token, err := readSmallGenerationFile(tokenPath, sha256.Size)
	if errors.Is(err, os.ErrNotExist) {
		token = make([]byte, sha256.Size)
		if _, err = io.ReadFull(rand.Reader, token); err != nil {
			return nil, err
		}
		if err = publishStorageGeneration(tokenPath, token); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if len(token) != sha256.Size {
		return nil, moerr.NewInvalidStateNoCtxf("invalid TAE storage generation token size %d", len(token))
	}
	// Also sync on a retry that observes the completed token after an earlier
	// directory-sync error. Reading it alone would not make that retry durable.
	dir, err := os.Open(canonical)
	if err != nil {
		return nil, err
	}
	if err = errors.Join(dir.Sync(), dir.Close()); err != nil {
		return nil, err
	}
	h := sha256.New()
	_, _ = h.Write([]byte("matrixone/tae/storage-generation/v1\x00"))
	_, _ = h.Write(token)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(machineID)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(canonical))
	return h.Sum(nil), nil
}

func publishStorageGeneration(name string, token []byte) error {
	// A fixed temporary name bounds crash residue to one file per directory.
	// The directory lock makes replacement of an interrupted write exclusive.
	tmp := name + ".tmp"
	if err := os.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp) }()
	_, writeErr := f.Write(token)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	if err = errors.Join(writeErr, f.Close()); err != nil {
		return err
	}
	// link publishes a fully synced file without replacing an existing token.
	return os.Link(tmp, name)
}
