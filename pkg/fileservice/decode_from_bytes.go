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

package fileservice

import (
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// CachedDataDecoder optionally supplies cache admission and scoped sharing for
// conversion of bytes already read by the caller. It must not perform storage I/O.
type CachedDataDecoder interface {
	DecodeFromBytes(context.Context, *IOVector, []byte) error
}

// DecodeFromBytes converts exactly one immutable physical range. data must match
// that range in the write-once file; neither it nor a view may escape conversion.
// The caller owns the complete result IOVector and must Release it. Errors after
// admission release this call's results; rejected preexisting ownership is left
// untouched. FileServices without this capability use owned conversion.
func DecodeFromBytes(ctx context.Context, fs FileService, v *IOVector, data []byte) error {
	if err := validateByteDecode(ctx, v, data); err != nil {
		return err
	}
	if decoder, ok := fs.(CachedDataDecoder); ok {
		return decoder.DecodeFromBytes(ctx, v, data)
	}
	return decodeEntryBytes(ctx, v, data, DefaultCacheDataAllocator())
}

func validateByteDecode(ctx context.Context, v *IOVector, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if v == nil || len(v.Entries) != 1 || len(v.Caches) != 0 {
		return moerr.NewInvalidInputNoCtx("byte decode requires one entry and no custom caches")
	}
	e := &v.Entries[0]
	if e.Offset < 0 || e.Size <= 0 || e.Size != int64(len(data)) || e.ToCacheData == nil ||
		e.Data != nil || e.CachedData != nil || e.done || e.releaseData != nil || e.releaseCachedData != nil || e.decodeLease != nil ||
		e.WriterForRead != nil || e.ReadCloserForRead != nil || e.ReaderForWrite != nil {
		return moerr.NewInvalidInputNoCtx("invalid byte decode input or nonempty result entry")
	}
	if _, err := ParsePath(v.FilePath); err != nil {
		return err
	}
	return nil
}

func decodeEntryBytes(ctx context.Context, v *IOVector, data []byte, allocator CacheDataAllocator) error {
	e := &v.Entries[0]
	result, err := e.ToCacheData(ctx, bytes.NewReader(data), data, allocator)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		if result != nil {
			result.Release()
		}
		return err
	}
	if result == nil {
		return moerr.NewInvalidStateNoCtx("byte converter returned no data")
	}
	e.CachedData, e.done = result, true
	return nil
}

func (s *S3FS) DecodeFromBytes(ctx context.Context, v *IOVector, data []byte) (err error) {
	if err := validateByteDecode(ctx, v, data); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			v.ReleaseReadResultOnError()
		}
	}()
	if _, err = parseFilePathAtService(v.FilePath, s.name); err != nil {
		return err
	}
	if err = s.ReadCache(ctx, v); err != nil || v.allDone() {
		return err
	}
	finish, err := s.prepareSharedDecode(v)
	if err != nil {
		return err
	}
	if finish == nil && s.decodedReads != nil {
		if !s.decodedReads.beginRead() {
			return sharedDecodeClosed()
		}
		finish = s.decodedReads.endRead
	}
	if finish != nil {
		defer finish() // transfer lease before the error cleanup above
	}
	if err = decodeEntryBytes(ctx, v, data, s); err != nil {
		return err
	}
	if s.memCache != nil {
		err = s.memCache.Update(ctx, v, false)
	}
	return err
}

func (s *subPathFS) DecodeFromBytes(ctx context.Context, v *IOVector, data []byte) error {
	if err := validateByteDecode(ctx, v, data); err != nil {
		return err
	}
	p, err := s.toUpstreamFilePath(v.FilePath)
	if err != nil {
		return err
	}
	sub := *v
	sub.FilePath = p
	return DecodeFromBytes(ctx, s.upstream, &sub, data)
}

func (f *FileServices) DecodeFromBytes(ctx context.Context, v *IOVector, data []byte) error {
	if err := validateByteDecode(ctx, v, data); err != nil {
		return err
	}
	p, err := parseFilePathAtService(v.FilePath, "")
	if err != nil {
		return err
	}
	if p.Service == "" {
		p.Service = f.defaultName
	}
	fs, err := Get[FileService](f, p.Service)
	if err != nil {
		return err
	}
	return DecodeFromBytes(ctx, fs, v, data)
}
