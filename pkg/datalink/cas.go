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
	"iter"
	"strconv"
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
// file service (without the service-name prefix). Layout:
// datalink_cas/<accountID>/<h2>/<hash>.
//
// The blob is namespaced by accountID so that a contenthash is not a global
// bearer capability: a hash known to one account cannot be used to read another
// account's pinned bytes, and there is no cross-account dedup visibility. The
// accountID must come from the trusted execution context (defines.GetAccountId),
// never from the datalink URL.
//
// hash must be a validated sha256 hex digest (see ValidateContentHash).
//
// The read path reads this key directly from the SHARED FileService rather than
// routing through GetForETL, because SHARED may be a plain FileService (e.g.
// LocalFS in standalone) that does not implement ETLFileService.
func CASKey(accountID uint32, hash string) string {
	return casPrefix + "/" + strconv.FormatUint(uint64(accountID), 10) + "/" + hash[:2] + "/" + hash
}

// casHashFromKey extracts the hash from a CAS key produced by CASKey, reporting
// whether p is such a key. Deriving ContentHash from an already-parsed MoPath
// (rather than re-parsing the URL) keeps ContentHash and MoPath consistent even
// for malformed input with duplicate/mixed-case contenthash params. The trailing
// path segment is always the hash regardless of the account/<h2> prefix.
func casHashFromKey(p string) (string, bool) {
	prefix := casPrefix + "/"
	if !strings.HasPrefix(p, prefix) {
		return "", false
	}
	rest := p[len(prefix):] // "<accountID>/<h2>/<hash>"
	if idx := strings.LastIndex(rest, "/"); idx >= 0 {
		return rest[idx+1:], true
	}
	return "", false
}

// CASPut writes data into the calling account's content-addressed namespace and
// returns its sha256 hex digest. The store is write-once and immutable: if the
// blob already exists the write is skipped (natural per-account dedup), so
// repeated Puts of identical bytes succeed without error and never overwrite the
// existing object.
func CASPut(ctx context.Context, fs fileservice.FileService, accountID uint32, data []byte) (string, error) {
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])

	vec := fileservice.IOVector{
		FilePath: CASKey(accountID, hash),
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

// CASExists reports whether a blob addressed by hash exists in the given
// account's namespace.
func CASExists(ctx context.Context, fs fileservice.FileService, accountID uint32, hash string) (bool, error) {
	if _, err := fs.StatFile(ctx, CASKey(accountID, hash)); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// CASEntry is a single blob enumerated from an account's CAS namespace.
type CASEntry struct {
	Hash string
	Key  string
	Size int64
}

// CASAccountPrefix returns the key prefix that holds all CAS blobs of an
// account: "datalink_cas/<accountID>/".
func CASAccountPrefix(accountID uint32) string {
	return casPrefix + "/" + strconv.FormatUint(uint64(accountID), 10) + "/"
}

// CASDelete removes a single content-addressed blob. A missing object is not an
// error: deletion is idempotent so a sweep can safely retry.
func CASDelete(ctx context.Context, fs fileservice.FileService, accountID uint32, hash string) error {
	if err := fs.Delete(ctx, CASKey(accountID, hash)); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil
		}
		return err
	}
	return nil
}

// CASListAccount enumerates every blob in the account's namespace. The CAS
// layout is datalink_cas/<accountID>/<h2>/<hash>, so it lists the account dir,
// descends one level into each <h2> bucket, and yields the trailing hash.
func CASListAccount(ctx context.Context, fs fileservice.FileService, accountID uint32) iter.Seq2[CASEntry, error] {
	prefix := CASAccountPrefix(accountID)
	return func(yield func(CASEntry, error) bool) {
		for bucket, err := range fs.List(ctx, prefix) {
			if err != nil {
				yield(CASEntry{}, err)
				return
			}
			if !bucket.IsDir {
				continue // CAS layout always nests under an <h2> bucket
			}
			bucketPath := prefix + bucket.Name + "/"
			for ent, err := range fs.List(ctx, bucketPath) {
				if err != nil {
					yield(CASEntry{}, err)
					return
				}
				if ent.IsDir {
					continue
				}
				e := CASEntry{Hash: ent.Name, Key: bucketPath + ent.Name, Size: ent.Size}
				if !yield(e, nil) {
					return
				}
			}
		}
	}
}

// CASDeleteAccountPrefix removes the entire CAS namespace of one account. Used
// by DROP ACCOUNT so a removed tenant leaves no pinned blobs behind.
func CASDeleteAccountPrefix(ctx context.Context, fs fileservice.FileService, accountID uint32) error {
	var keys []string
	for e, err := range CASListAccount(ctx, fs, accountID) {
		if err != nil {
			return err
		}
		keys = append(keys, e.Key)
	}
	if len(keys) == 0 {
		return nil
	}
	return fs.Delete(ctx, keys...)
}
