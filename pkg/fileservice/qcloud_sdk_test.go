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

package fileservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/tencentyun/cos-go-sdk-v5"
)

func TestQCloudSDK(t *testing.T) {

	t.Run("object storage", func(t *testing.T) {
		for _, args := range objectStorageArgumentsForTest("test", t) {
			if !strings.Contains(args.Endpoint, "myqcloud") {
				continue
			}

			t.Run(fmt.Sprintf("%s %s", args.Name, args.Bucket), func(t *testing.T) {

				testObjectStorage(t, "qcloud", func(t *testing.T) *QCloudSDK {
					args.KeyPrefix = fmt.Sprintf("%v", rand.Int64())
					ret, err := NewQCloudSDK(
						context.Background(),
						args,
						nil,
					)
					if err != nil {
						t.Fatal(err)
					}
					return ret
				})

			})
		}
	})

	t.Run("file service", func(t *testing.T) {
		for _, args := range objectStorageArgumentsForTest("test", t) {
			if !strings.Contains(args.Endpoint, "myqcloud") {
				continue
			}

			t.Run(fmt.Sprintf("%s %s", args.Name, args.Bucket), func(t *testing.T) {

				t.Run("file service", func(t *testing.T) {
					testFileService(t, 0, func(name string) FileService {
						args.Name = name
						args.KeyPrefix = fmt.Sprintf("%v", rand.Int64())
						ret, err := NewS3FS(
							context.Background(),
							args,
							DisabledCacheConfig,
							nil,
							true,
							true,
						)
						if err != nil {
							t.Fatal(err)
						}
						return ret
					})
				})

			})
		}
	})

}

func TestQCloudSDKWriteRetriesSeekablePut(t *testing.T) {
	data := bytes.Repeat([]byte("x"), int(smallObjectThreshold))
	size := int64(len(data))
	transport := &qcloudRetryPutTransport{}
	reader := &trackingSeekReader{Reader: bytes.NewReader(data)}

	baseURL, err := url.Parse("http://cos.local")
	if err != nil {
		t.Fatal(err)
	}
	client := cos.NewClient(
		&cos.BaseURL{BucketURL: baseURL},
		&http.Client{Transport: transport},
	)
	client.Conf.EnableCRC = false
	client.Conf.RetryOpt.Count = 0

	sdk := &QCloudSDK{
		name:   "qcloud-retry-test",
		client: client,
	}
	if err := sdk.Write(context.Background(), "object", reader, &size, nil); err != nil {
		t.Fatal(err)
	}

	if transport.calls != 2 {
		t.Fatalf("expected 2 put attempts, got %d", transport.calls)
	}
	for i, body := range transport.bodies {
		if !bytes.Equal(body, data) {
			t.Fatalf("attempt %d body mismatch: got %d bytes, want %d", i+1, len(body), len(data))
		}
	}
	if reader.seekStartCount == 0 {
		t.Fatalf("expected QCloudSDK.Write to seek back before retry")
	}
}

type qcloudRetryPutTransport struct {
	calls  int
	bodies [][]byte
}

func (t *qcloudRetryPutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.calls++
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	t.bodies = append(t.bodies, body)
	if t.calls == 1 {
		return nil, errors.New("write: connection reset by peer")
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Request:    req,
	}, nil
}

type trackingSeekReader struct {
	*bytes.Reader
	seekStartCount int
}

func (r *trackingSeekReader) Seek(offset int64, whence int) (int64, error) {
	if offset == 0 && whence == io.SeekStart {
		r.seekStartCount++
	}
	return r.Reader.Seek(offset, whence)
}

func TestQCloudSDKWriteNonSeekableFallbackPreservesSizeHint(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		server, state := newMockCOSServer(t, 0)
		defer server.Close()
		state.uploadID = "cos-size-mismatch-empty"

		sdk := newTestCOSClient(t, server)
		size := int64(smallObjectThreshold)
		err := sdk.Write(context.Background(), "object", nonSeekReader{r: bytes.NewReader(nil)}, &size, nil)
		if !moerr.IsMoErrCode(err, moerr.ErrSizeNotMatch) {
			t.Fatalf("expected size mismatch, got %v", err)
		}
		if state.putCount != 0 || len(state.parts) != 0 {
			t.Fatalf("expected no successful upload for empty short reader")
		}
	})

	t.Run("short", func(t *testing.T) {
		server, _ := newMockCOSServer(t, 0)
		defer server.Close()

		sdk := newTestCOSClient(t, server)
		size := int64(smallObjectThreshold)
		err := sdk.Write(context.Background(), "object", nonSeekReader{r: bytes.NewReader([]byte("short"))}, &size, nil)
		if !moerr.IsMoErrCode(err, moerr.ErrSizeNotMatch) {
			t.Fatalf("expected size mismatch, got %v", err)
		}
	})
}

type nonSeekReader struct {
	r io.Reader
}

func (r nonSeekReader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}
