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

package icebergio

import (
	"bytes"
	"context"
	"io"
	"iter"
	"math"
	"net/http"
	pathpkg "path"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const maxSignedHTTPMaterializedReadBytes int64 = 512 << 20

type SignedHTTPFileServiceBuilder struct {
	HTTPClient               *http.Client
	MaxMaterializedReadBytes int64
}

func (b SignedHTTPFileServiceBuilder) Build(ctx context.Context, scope ObjectScope, signed SignedRequest) (fileservice.ETLFileService, string, error) {
	name := "iceberg-signed-http-" + api.PathHash(scope.StorageLocation)
	client := b.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	maxMaterializedReadBytes := b.MaxMaterializedReadBytes
	if maxMaterializedReadBytes <= 0 || maxMaterializedReadBytes > maxSignedHTTPMaterializedReadBytes {
		maxMaterializedReadBytes = maxSignedHTTPMaterializedReadBytes
	}
	return &signedHTTPFileService{
		name:                     name,
		client:                   client,
		headers:                  cloneStringMap(signed.Headers),
		maxMaterializedReadBytes: maxMaterializedReadBytes,
	}, strings.TrimSpace(signed.URL), nil
}

type signedHTTPFileService struct {
	name                     string
	client                   *http.Client
	headers                  map[string]string
	maxMaterializedReadBytes int64
}

func (s *signedHTTPFileService) Name() string {
	return s.name
}

func (s *signedHTTPFileService) Write(ctx context.Context, vector fileservice.IOVector) error {
	target := strings.TrimSpace(vector.FilePath)
	if target == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg signed HTTP write requires URL", nil))
	}
	if len(vector.Entries) != 1 {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg signed HTTP write requires exactly one entry", map[string]string{
			"signed_url": RedactObjectPath(target),
		}))
	}
	entry := vector.Entries[0]
	if entry.Offset != 0 {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg signed HTTP write requires offset 0", map[string]string{
			"signed_url": RedactObjectPath(target),
		}))
	}
	body := append([]byte(nil), entry.Data...)
	if entry.ReaderForWrite != nil {
		reader := io.Reader(entry.ReaderForWrite)
		if entry.Size >= 0 {
			limit := entry.Size
			if limit < math.MaxInt64 {
				limit++
			}
			reader = io.LimitReader(reader, limit)
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			return api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg signed HTTP write payload read failed", map[string]string{
				"signed_url": RedactObjectPath(target),
			}, err))
		}
		body = data
	}
	if entry.Size >= 0 && int64(len(body)) != entry.Size {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg signed HTTP write size mismatch", map[string]string{
			"signed_url": RedactObjectPath(target),
		}))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, target, bytes.NewReader(body))
	if err != nil {
		return api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg signed HTTP write URL is invalid", map[string]string{
			"signed_url": RedactObjectPath(target),
		}, err))
	}
	s.addSignedHeaders(req)
	req.ContentLength = int64(len(body))
	resp, err := s.client.Do(req)
	if err != nil {
		return api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg signed HTTP write failed", map[string]string{
			"signed_url": RedactObjectPath(target),
		}, err))
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg signed HTTP write returned non-success status", map[string]string{
			"signed_url": RedactObjectPath(target),
			"status":     strconv.Itoa(resp.StatusCode),
		}))
	}
	return nil
}

func (s *signedHTTPFileService) Read(ctx context.Context, vector *fileservice.IOVector) error {
	if vector == nil || len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
	}
	remainingMaterializedBytes := s.maxMaterializedReadBytes
	if remainingMaterializedBytes <= 0 || remainingMaterializedBytes > maxSignedHTTPMaterializedReadBytes {
		remainingMaterializedBytes = maxSignedHTTPMaterializedReadBytes
	}
	for i := range vector.Entries {
		if err := s.readEntry(ctx, strings.TrimSpace(vector.FilePath), &vector.Entries[i], &remainingMaterializedBytes); err != nil {
			return err
		}
	}
	return nil
}

func (s *signedHTTPFileService) ReadCache(ctx context.Context, vector *fileservice.IOVector) error {
	return nil
}

func (s *signedHTTPFileService) List(ctx context.Context, dirPath string) iter.Seq2[*fileservice.DirEntry, error] {
	return func(yield func(*fileservice.DirEntry, error) bool) {
		yield(nil, moerr.NewNotSupportedNoCtx("iceberg signed HTTP FileService does not support list"))
	}
}

func (s *signedHTTPFileService) Delete(ctx context.Context, filePaths ...string) error {
	return moerr.NewNotSupportedNoCtx("iceberg signed HTTP FileService is read-only")
}

func (s *signedHTTPFileService) StatFile(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
	target := strings.TrimSpace(filePath)
	if target == "" {
		return nil, api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg signed HTTP stat requires URL", nil))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg signed HTTP stat URL is invalid", map[string]string{
			"signed_url": RedactObjectPath(target),
		}, err))
	}
	s.addSignedHeaders(req)
	req.Header.Set("Range", "bytes=0-0")
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg signed HTTP stat failed", map[string]string{
			"signed_url": RedactObjectPath(target),
		}, err))
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusPartialContent:
		if size, ok := contentRangeSize(resp.Header.Get("Content-Range")); ok {
			return &fileservice.DirEntry{Name: pathpkg.Base(target), Size: size}, nil
		}
	case http.StatusOK:
		if resp.ContentLength >= 0 {
			return &fileservice.DirEntry{Name: pathpkg.Base(target), Size: resp.ContentLength}, nil
		}
	case http.StatusRequestedRangeNotSatisfiable:
		if size, ok := contentRangeSize(resp.Header.Get("Content-Range")); ok {
			return &fileservice.DirEntry{Name: pathpkg.Base(target), Size: size}, nil
		}
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return nil, api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg signed HTTP stat returned unusable response", map[string]string{
		"signed_url":    RedactObjectPath(target),
		"status":        strconv.Itoa(resp.StatusCode),
		"content_range": resp.Header.Get("Content-Range"),
		"content_len":   strconv.FormatInt(resp.ContentLength, 10),
		"body_hash":     api.PathHash(string(body)),
	}))
}

func (s *signedHTTPFileService) PrefetchFile(ctx context.Context, filePath string) error {
	return nil
}

func (s *signedHTTPFileService) Cost() *fileservice.CostAttr {
	return &fileservice.CostAttr{List: fileservice.CostHigh}
}

func (s *signedHTTPFileService) Close(ctx context.Context) {
}

func (s *signedHTTPFileService) ETLCompatible() {
}

func (s *signedHTTPFileService) readEntry(ctx context.Context, target string, entry *fileservice.IOEntry, remainingMaterializedBytes *int64) (err error) {
	if target == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg signed HTTP read requires URL", nil))
	}
	if entry.Offset < 0 || entry.Size < -1 || (entry.Size > 0 && entry.Offset > math.MaxInt64-entry.Size) {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg signed HTTP read range is invalid", map[string]string{
			"signed_url": RedactObjectPath(target),
		}))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg signed HTTP request URL is invalid", map[string]string{
			"signed_url": RedactObjectPath(target),
		}, err))
	}
	s.addSignedHeaders(req)
	if rangeHeader := httpRangeHeader(entry.Offset, entry.Size); rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg signed HTTP read failed", map[string]string{
			"signed_url": RedactObjectPath(target),
		}, err))
	}
	closeBody := true
	defer func() {
		if closeBody {
			_ = resp.Body.Close()
		}
	}()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg signed HTTP read returned non-success status", map[string]string{
			"signed_url": RedactObjectPath(target),
			"status":     strconv.Itoa(resp.StatusCode),
			"body_hash":  api.PathHash(string(body)),
		}))
	}
	if resp.StatusCode == http.StatusOK && entry.Offset > 0 {
		if _, err := io.CopyN(io.Discard, resp.Body, entry.Offset); err != nil {
			return io.ErrUnexpectedEOF
		}
	}
	reader := io.Reader(resp.Body)
	if entry.Size >= 0 {
		reader = io.LimitReader(reader, entry.Size)
	}
	// ReadCloserForRead means the caller has opted into streaming. Transfer
	// response-body ownership instead of materializing the object in the CN.
	// The combined writer/cache forms retain their historical eager semantics.
	if entry.ReadCloserForRead != nil && entry.WriterForRead == nil && entry.ToCacheData == nil {
		entry.Data = nil
		stream := &signedHTTPResponseReadCloser{reader: reader, body: resp.Body}
		runtime.SetFinalizer(stream, func(stream *signedHTTPResponseReadCloser) {
			_ = stream.Close()
		})
		*entry.ReadCloserForRead = stream
		closeBody = false
		return nil
	}
	if entry.WriterForRead != nil && entry.ReadCloserForRead == nil && entry.ToCacheData == nil {
		entry.Data = nil
		n, err := io.Copy(entry.WriterForRead, reader)
		if err != nil {
			return err
		}
		if entry.Size >= 0 && n < entry.Size {
			return io.ErrUnexpectedEOF
		}
		return nil
	}

	maxBytes := *remainingMaterializedBytes
	if entry.Size > maxBytes {
		return s.materializedReadTooLarge(ctx, target, maxBytes)
	}
	if entry.Size < 0 {
		reader = io.LimitReader(reader, maxBytes+1)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return api.ToMOErr(ctx, api.WrapError(api.ErrObjectIO, "Iceberg signed HTTP response read failed", map[string]string{
			"signed_url": RedactObjectPath(target),
		}, err))
	}
	if int64(len(data)) > maxBytes {
		return s.materializedReadTooLarge(ctx, target, maxBytes)
	}
	if entry.Size >= 0 {
		if int64(len(data)) < entry.Size {
			return io.ErrUnexpectedEOF
		}
	} else {
		entry.Size = int64(len(data))
	}
	*remainingMaterializedBytes -= int64(len(data))
	return fillReadEntry(ctx, entry, data)
}

func (s *signedHTTPFileService) materializedReadTooLarge(ctx context.Context, target string, maxBytes int64) error {
	return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg signed HTTP response is too large to materialize", map[string]string{
		"signed_url": RedactObjectPath(target),
		"max_bytes":  strconv.FormatInt(maxBytes, 10),
	}))
}

type signedHTTPResponseReadCloser struct {
	reader io.Reader
	body   io.Closer
	once   sync.Once
	err    error
}

func (r *signedHTTPResponseReadCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *signedHTTPResponseReadCloser) Close() error {
	r.once.Do(func() {
		r.err = r.body.Close()
		runtime.SetFinalizer(r, nil)
	})
	return r.err
}

func (s *signedHTTPFileService) addSignedHeaders(req *http.Request) {
	for key, value := range s.headers {
		if strings.EqualFold(key, "host") {
			req.Host = strings.TrimSpace(value)
			continue
		}
		req.Header.Set(key, value)
	}
}

func fillReadEntry(ctx context.Context, entry *fileservice.IOEntry, data []byte) error {
	if entry.WriterForRead != nil {
		if _, err := entry.WriterForRead.Write(data); err != nil {
			return err
		}
	}
	if entry.ReadCloserForRead != nil {
		*entry.ReadCloserForRead = io.NopCloser(bytes.NewReader(data))
	}
	if cap(entry.Data) >= len(data) {
		entry.Data = entry.Data[:len(data)]
		copy(entry.Data, data)
	} else {
		// data is freshly owned by this read. Transfer it directly to avoid a
		// second full-response allocation at the materialization boundary.
		entry.Data = data
	}
	if entry.ToCacheData != nil {
		cacheData, err := entry.ToCacheData(ctx, bytes.NewReader(data), data, fileservice.DefaultCacheDataAllocator())
		if err != nil {
			return err
		}
		entry.CachedData = cacheData
	}
	return nil
}

func httpRangeHeader(offset, size int64) string {
	if offset < 0 || size == 0 || size < -1 {
		return ""
	}
	if size < 0 {
		if offset == 0 {
			return ""
		}
		return "bytes=" + strconv.FormatInt(offset, 10) + "-"
	}
	return "bytes=" + strconv.FormatInt(offset, 10) + "-" + strconv.FormatInt(offset+size-1, 10)
}

func contentRangeSize(value string) (int64, bool) {
	value = strings.TrimSpace(value)
	slash := strings.LastIndex(value, "/")
	if slash < 0 || slash == len(value)-1 {
		return 0, false
	}
	raw := strings.TrimSpace(value[slash+1:])
	if raw == "*" || raw == "" {
		return 0, false
	}
	size, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || size < 0 {
		return 0, false
	}
	return size, true
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

var _ SignedFileServiceBuilder = SignedHTTPFileServiceBuilder{}.Build
var _ fileservice.ETLFileService = new(signedHTTPFileService)
