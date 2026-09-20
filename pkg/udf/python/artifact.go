// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package python

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/udf/udferr"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/udf"
)

// DefaultMaxArtifactBytes bounds the inline source bundle used by the current
// Python contract. The bound is applied before a write and before a read, so a
// corrupt or malicious object cannot turn an artifact lookup into an
// unbounded allocation.
const DefaultMaxArtifactBytes int64 = 1 << 20

// ArtifactResolver supplies the immutable source bytes for an exact catalog
// artifact reference. The resolver is deliberately keyed by account and
// digest: a digest alone must not become a cross-account capability.
type ArtifactResolver interface {
	Resolve(ctx context.Context, accountID uint64, handler, digest string) (string, error)
}

// ArtifactPublisher publishes an immutable artifact before its catalog
// revision becomes visible. Implementations must make a digest collision or a
// corrupt pre-existing object an error; replacing an object at the same path
// would break old plans that still reference that digest.
type ArtifactPublisher interface {
	Publish(ctx context.Context, accountID uint64, handler, source string) (string, error)
}

// ArtifactStore is the current FileService-backed artifact implementation.
// Resolver and publisher are separate interfaces so execution only needs read
// capability and cannot accidentally publish from the hot path.
type ArtifactStore interface {
	ArtifactResolver
	ArtifactPublisher
}

// FileArtifactStore stores content addressed inline source under the shared
// FileService. The account component is part of the object path even though
// the content digest is global, preventing one account from using another
// account's artifact as an execution capability.
type FileArtifactStore struct {
	fs      fileservice.FileService
	maxSize int64
}

var _ ArtifactStore = (*FileArtifactStore)(nil)

func NewFileArtifactStore(fs fileservice.FileService, maxSize int64) (*FileArtifactStore, error) {
	if fs == nil {
		return nil, udferr.Newf("RESOURCE_UNAVAILABLE: Python artifact store has no FileService")
	}
	shared, err := fileservice.Get[fileservice.FileService](fs, defines.SharedFileServiceName)
	if err != nil {
		return nil, errutil.Wrapf(err, "RESOURCE_UNAVAILABLE: Python artifact store has no shared FileService")
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxArtifactBytes
	}
	if maxSize > DefaultMaxArtifactBytes {
		return nil, udferr.Newf("UNSUPPORTED_ROUTINE_VERSION: Python artifact limit %d exceeds the current contract limit %d", maxSize, DefaultMaxArtifactBytes)
	}
	return &FileArtifactStore{fs: shared, maxSize: maxSize}, nil
}

func (s *FileArtifactStore) Publish(ctx context.Context, accountID uint64, handler, source string) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	digest, err := validateArtifactInput(accountID, handler, source, s.maxSize)
	if err != nil {
		return "", err
	}
	path := artifactPath(accountID, digest)
	data := []byte(source)
	// FileService implementations may retain the caller's IOEntry until the
	// write returns. Give the store its own immutable backing so later caller
	// mutation cannot alter the bytes whose digest is being published.
	owned := append([]byte(nil), data...)
	err = s.fs.Write(ctx, fileservice.IOVector{
		FilePath: path,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(owned)), Data: owned}},
	})
	if err == nil {
		return digest, nil
	}
	if !moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
		return "", errutil.Wrapf(err, "python artifact publish %s", digest)
	}
	// A concurrent or retrying publisher is successful only after reading and
	// validating the existing object. This closes the write-once collision
	// boundary instead of treating path existence as proof of content identity.
	existing, resolveErr := s.Resolve(ctx, accountID, handler, digest)
	if resolveErr != nil {
		return "", errutil.Wrapf(resolveErr, "python artifact publish %s found an invalid existing object", digest)
	}
	if existing != source {
		return "", udferr.Newf("python artifact publish %s encountered a digest collision", digest)
	}
	return digest, nil
}

func (s *FileArtifactStore) Resolve(ctx context.Context, accountID uint64, handler, digest string) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(handler) == "" {
		return "", udferr.Newf("UNSUPPORTED_ROUTINE_VERSION: Python artifact has no handler")
	}
	if !udf.IsSHA256Digest(digest) {
		return "", udferr.Newf("UNSUPPORTED_ROUTINE_VERSION: Python artifact digest is invalid")
	}
	path := artifactPath(accountID, digest)
	entry, err := s.fs.StatFile(ctx, path)
	if err != nil {
		return "", errutil.Wrapf(err, "RESOURCE_UNAVAILABLE: Python artifact %s is unavailable", digest)
	}
	if entry == nil || entry.IsDir || entry.Size <= 0 || entry.Size > s.maxSize {
		return "", udferr.Newf("UNSUPPORTED_ROUTINE_VERSION: Python artifact %s has invalid size", digest)
	}
	data := make([]byte, entry.Size)
	readVector := &fileservice.IOVector{
		FilePath: path,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: entry.Size, Data: data}},
	}
	if err := s.fs.Read(ctx, readVector); err != nil {
		return "", errutil.Wrapf(err, "RESOURCE_UNAVAILABLE: Python artifact %s cannot be read", digest)
	}
	if len(readVector.Entries) != 1 || int64(len(readVector.Entries[0].Data)) != entry.Size {
		return "", udferr.Newf("UNSUPPORTED_ROUTINE_VERSION: Python artifact %s changed size while reading", digest)
	}
	source := string(readVector.Entries[0].Data)
	if !utf8.ValidString(source) {
		return "", udferr.Newf("UNSUPPORTED_ROUTINE_VERSION: Python artifact %s is not valid UTF-8", digest)
	}
	if udf.PythonInlineArtifactDigest(handler, source) != digest {
		return "", udferr.Newf("UNSUPPORTED_ROUTINE_VERSION: Python artifact %s content digest mismatch", digest)
	}
	return source, nil
}

func validateArtifactInput(accountID uint64, handler, source string, maxSize int64) (string, error) {
	if maxSize <= 0 {
		return "", udferr.Newf("RESOURCE_UNAVAILABLE: Python artifact store has an invalid size limit")
	}
	if strings.TrimSpace(handler) == "" {
		return "", udferr.Newf("UNSUPPORTED_ROUTINE_VERSION: Python artifact has no handler")
	}
	if source == "" {
		return "", udferr.Newf("UNSUPPORTED_ROUTINE_VERSION: Python artifact source is empty")
	}
	if !utf8.ValidString(source) {
		return "", udferr.Newf("UNSUPPORTED_ROUTINE_VERSION: Python artifact source is not valid UTF-8")
	}
	if int64(len([]byte(source))) > maxSize {
		return "", udferr.Newf("RESOURCE_EXHAUSTED: Python artifact exceeds %d bytes", maxSize)
	}
	return udf.PythonInlineArtifactDigest(handler, source), nil
}

func (s *FileArtifactStore) validate() error {
	if s == nil || s.fs == nil {
		return udferr.Newf("RESOURCE_UNAVAILABLE: Python artifact store has no FileService")
	}
	if s.maxSize <= 0 {
		return udferr.Newf("RESOURCE_UNAVAILABLE: Python artifact store has an invalid size limit")
	}
	return nil
}

func artifactPath(accountID uint64, digest string) string {
	return fileservice.JoinPath(
		defines.SharedFileServiceName,
		fmt.Sprintf("udf-artifacts/%d/%s", accountID, digest),
	)
}
