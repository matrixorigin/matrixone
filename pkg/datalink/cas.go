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

package datalink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const (
	// ContentHashKey is the datalink URL query parameter that marks a pinned
	// value. Its value is the sha256 hex digest of the referenced bytes.
	ContentHashKey = "contenthash"

	// casPrefix is the reserved key prefix for content-addressed datalink blobs
	// inside the SHARED file service.
	casPrefix = "datalink_cas"
)

// ValidateContentHash checks that hash is a well-formed sha256 hex digest, so
// that callers (e.g. CASKey) can safely assume a fixed-length lower-case hex.
func ValidateContentHash(hash string) error {
	if len(hash) != hex.EncodedLen(sha256.Size) {
		return moerr.NewInternalErrorNoCtxf(
			"invalid datalink contenthash length %d, want %d", len(hash), hex.EncodedLen(sha256.Size))
	}
	if _, err := hex.DecodeString(hash); err != nil {
		return moerr.NewInternalErrorNoCtxf("invalid datalink contenthash %q: not hex", hash)
	}
	return nil
}

// CASKey returns the storage key of a content-addressed blob within the SHARED
// file service (without the service-name prefix). Layout: datalink_cas/<h2>/<hash>.
// hash must be a validated sha256 hex digest (see ValidateContentHash).
//
// The read path reads this key directly from the SHARED FileService rather than
// routing through GetForETL, because SHARED may be a plain FileService (e.g.
// LocalFS in standalone) that does not implement ETLFileService.
func CASKey(hash string) string {
	return casPrefix + "/" + hash[:2] + "/" + hash
}

// casHashFromKey extracts the hash from a CAS key produced by CASKey, reporting
// whether p is such a key. Deriving ContentHash from an already-parsed MoPath
// (rather than re-parsing the URL) keeps ContentHash and MoPath consistent even
// for malformed input with duplicate/mixed-case contenthash params.
func casHashFromKey(p string) (string, bool) {
	prefix := casPrefix + "/"
	if !strings.HasPrefix(p, prefix) {
		return "", false
	}
	rest := p[len(prefix):] // "<h2>/<hash>"
	if idx := strings.LastIndex(rest, "/"); idx >= 0 {
		return rest[idx+1:], true
	}
	return "", false
}

// CASPut writes data into the content-addressed store and returns its sha256
// hex digest. The store is write-once and immutable: if the blob already exists
// the write is skipped (natural dedup), so repeated Puts of identical bytes
// succeed without error and never overwrite the existing object.
func CASPut(ctx context.Context, fs fileservice.FileService, data []byte) (string, error) {
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])

	vec := fileservice.IOVector{
		FilePath: CASKey(hash),
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   int64(len(data)),
				Data:   data,
			},
		},
	}
	if err := fs.Write(ctx, vec); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
			return hash, nil
		}
		return "", err
	}
	return hash, nil
}

// CASExists reports whether a blob addressed by hash exists in the store.
func CASExists(ctx context.Context, fs fileservice.FileService, hash string) (bool, error) {
	if _, err := fs.StatFile(ctx, CASKey(hash)); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
