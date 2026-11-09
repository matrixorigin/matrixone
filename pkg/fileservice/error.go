// Copyright 2022 Matrix Origin
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
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"syscall"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	smithy "github.com/aws/smithy-go"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/minio/minio-go/v7"
	"github.com/tencentyun/cos-go-sdk-v5"
)

func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	var cosRetryErr *cos.RetryError
	if errors.As(err, &cosRetryErr) && len(cosRetryErr.Errs) > 0 {
		for _, innerErr := range cosRetryErr.Errs {
			if IsRetryableError(innerErr) {
				return true
			}
		}
		return false
	}

	var cosErr *cos.ErrorResponse
	if errors.As(err, &cosErr) && cosErr.Response != nil {
		statusCode := cosErr.Response.StatusCode
		if statusCode >= http.StatusInternalServerError ||
			statusCode == http.StatusTooManyRequests {
			return true
		}
	}

	str := err.Error()

	// unexpected EOF
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// net timeout
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	// match exact string
	switch str {
	case "connection reset by peer",
		"connection timed out":
		return true
	}

	// match sub-string
	if strings.Contains(str, "unexpected EOF") ||
		strings.Contains(str, "connection reset by peer") ||
		strings.Contains(str, "connection timed out") ||
		strings.Contains(str, "dial tcp: lookup") ||
		strings.Contains(str, "server closed idle connection") ||
		strings.Contains(str, "server closed connection") ||
		strings.Contains(str, "i/o timeout") ||
		strings.Contains(str, "server closed idle connection") ||
		strings.Contains(str, "write: broken pipe") ||
		strings.Contains(str, "TLS handshake timeout") ||
		strings.Contains(str, "replication in progress") || // HDFS error
		strings.Contains(str, "use of closed network connection") {
		return true
	}

	return false
}

// IsRetryableStartupError extends the steady-state file-service retry policy
// with failures that are transient while a service endpoint is coming up.
// Startup callers use this policy; regular file-service operations keep their
// existing bounded retry behavior and fail fast on an unreachable endpoint.
func IsRetryableStartupError(err error) bool {
	if err == nil {
		return false
	}
	if IsRetryableError(err) {
		return true
	}

	if statusCode, ok := objectStorageStatusCode(err); ok {
		if statusCode == http.StatusRequestTimeout ||
			statusCode == http.StatusTooManyRequests ||
			statusCode >= http.StatusInternalServerError {
			return true
		}
	}
	if code, ok := objectStorageErrorCode(err); ok && retryableObjectStorageErrorCode(code) {
		return true
	}

	str := err.Error()
	// MinIO's broad network classifier treats endpoint protocol mismatches as
	// host-down, although its request retryer correctly rejects both cases.
	if strings.Contains(str, "server gave HTTP response to HTTPS client") ||
		strings.Contains(str, "Client sent an HTTP request to an HTTPS server") {
		return false
	}

	// MinIO wraps an EOF returned by its HTTP transport in a url.Error whose
	// inner error has no typed sentinel. Use the SDK's canonical classifier for
	// that case, but keep certificate verification failures fail-fast.
	var certificateErr *tls.CertificateVerificationError
	if errors.As(err, &certificateErr) {
		return false
	}
	if minio.IsNetworkOrHostDown(err, false) {
		return true
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}

	return errors.Is(err, syscall.ECONNREFUSED)
}

// objectStorageStatusCode returns the HTTP status of the object-storage response that err
// reports.
func objectStorageStatusCode(err error) (int, bool) {
	var minioErr minio.ErrorResponse
	if errors.As(err, &minioErr) {
		return minioErr.StatusCode, true
	}
	var awsErr interface{ HTTPStatusCode() int }
	if errors.As(err, &awsErr) {
		return awsErr.HTTPStatusCode(), true
	}
	var ossErr oss.ServiceError
	if errors.As(err, &ossErr) {
		return ossErr.StatusCode, true
	}
	var cosErr *cos.ErrorResponse
	if errors.As(err, &cosErr) && cosErr.Response != nil {
		return cosErr.Response.StatusCode, true
	}
	return 0, false
}

// objectStorageErrorCode returns the API error code of the object-storage response that err
// reports, such as RequestTimeout or SlowDown.
func objectStorageErrorCode(err error) (string, bool) {
	var minioErr minio.ErrorResponse
	if errors.As(err, &minioErr) {
		return minioErr.Code, true
	}
	var awsErr smithy.APIError
	if errors.As(err, &awsErr) {
		return awsErr.ErrorCode(), true
	}
	var ossErr oss.ServiceError
	if errors.As(err, &ossErr) {
		return ossErr.Code, true
	}
	var cosErr *cos.ErrorResponse
	if errors.As(err, &cosErr) {
		return cosErr.Code, true
	}
	return "", false
}

// retryableObjectStorageErrorCode reports whether the AWS SDK retryer retries an API error
// with code: a request timeout or a throttling response.
func retryableObjectStorageErrorCode(code string) bool {
	if _, ok := retry.DefaultRetryableErrorCodes[code]; ok {
		return true
	}
	_, ok := retry.DefaultThrottleErrorCodes[code]
	return ok
}

type errorStr string

func (e errorStr) Error() string {
	return string(e)
}

type throwError struct {
	err error
}

func throw(err error) {
	panic(throwError{
		err: err,
	})
}

func catch(ptr *error) {
	p := recover()
	if p == nil {
		return
	}
	e, ok := p.(throwError)
	if !ok {
		panic(p)
	} else {
		*ptr = e.err
	}
}

var httpBadLengthPattern = regexp.MustCompile(`transport connection broken: http: ContentLength=[0-9]* with Body length [0-9]*`)

func wrapSizeMismatchErr(p *error) {
	if *p == nil {
		return
	}
	str := (*p).Error()

	if strings.Contains(str, "size does not match") ||
		httpBadLengthPattern.MatchString(str) {
		*p = moerr.NewSizeNotMatchNoCtx("")
	}

}

type diskFullKind int

const (
	notDiskFull diskFullKind = iota
	// diskQuotaExceeded is EDQUOT: a quota on the cache's own directory or
	// owner is exhausted, so freeing cache files always makes room.
	diskQuotaExceeded
	// diskNoSpace is ENOSPC: blocks or inodes of the filesystem are exhausted,
	// or a structural per-directory limit was hit. Only the former is relieved
	// by evicting cache files.
	diskNoSpace
)

func classifyDiskFull(err error) diskFullKind {
	if err == nil {
		return notDiskFull
	}
	if errors.Is(err, syscall.EDQUOT) {
		return diskQuotaExceeded
	}
	if errors.Is(err, syscall.ENOSPC) {
		return diskNoSpace
	}
	// Some layers flatten errors to strings.
	str := err.Error()
	if strings.Contains(str, "disk quota exceeded") {
		return diskQuotaExceeded
	}
	if strings.Contains(str, "no space left on device") {
		return diskNoSpace
	}
	return notDiskFull
}
