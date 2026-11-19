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

package fulltext2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestDatalinkError(t *testing.T) {
	const url = "stage://s/docs/a.pdf"
	require.NoError(t, DatalinkError(url, nil))
	for _, err := range []error{
		moerr.NewBadConfigNoCtx("Stage s not found"),
		moerr.NewFileNotFoundNoCtx("docs/a.pdf"),
		moerr.NewNYINoCtx("unsupported url scheme ftp"),
		moerr.NewInternalErrorNoCtx("docx: not a zip file"),
		errors.New("pdf: malformed header"),
		&smithy.OperationError{ServiceID: "S3", OperationName: "GetObject", Err: &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusBadRequest}},
				Err:      &smithy.GenericAPIError{Code: "InvalidRequest", Message: "bad range"},
			},
		}},
	} {
		got := DatalinkError(url, err)
		require.True(t, moerr.IsMoErrCode(got, moerr.ErrIndexDatalink), "%v", err)
		require.Contains(t, got.Error(), url+": "+err.Error())
	}
	for _, err := range []error{
		context.DeadlineExceeded,
		fmt.Errorf("read: %w", context.Canceled),
		io.ErrUnexpectedEOF,
		errors.New("dial tcp: i/o timeout"),
		moerr.NewInternalErrorNoCtx("read tcp: connection reset by peer"),
		moerr.NewTxnClosedNoCtx(nil),
		&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
		&net.DNSError{Err: "no such host", Name: "minio"},
		moerr.NewNotSupportedNoCtx("asynchronous indexes require remote Datalink sources"),
		&smithy.OperationError{ServiceID: "S3", OperationName: "GetObject", Err: &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusServiceUnavailable}},
				Err:      errors.New("api error SlowDown"),
			},
		}},
		&smithy.OperationError{ServiceID: "S3", OperationName: "GetObject", Err: &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusBadRequest}},
				Err:      &smithy.GenericAPIError{Code: "RequestTimeout", Message: "idle timeout"},
			},
		}},
	} {
		require.Equal(t, err, DatalinkError(url, err), "%v is retried", err)
	}
}

func TestJSONError(t *testing.T) {
	require.NoError(t, JSONError(nil))
	err := JSONError(moerr.NewInvalidInputNoCtx("json text {"))
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrIndexInvalidJSON))
	require.Contains(t, err.Error(), "json text {")
}
