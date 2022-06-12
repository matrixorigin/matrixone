// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func TestErrorConversion(t *testing.T) {
	mappings := getErrorToCodeMapping()
	for _, rec := range mappings {
		if rec.reverse {
			code, msg := toErrorCode(rec.err)
			resp := pb.Response{
				ErrorCode:    code,
				ErrorMessage: msg,
			}
			err := toError(resp)
			assert.Equal(t, err, rec.err)
		}
	}
}

func TestUnknownErrorIsHandled(t *testing.T) {
	err := errors.New("test error")
	code, str := toErrorCode(err)
	assert.Equal(t, pb.ErrorCode_OtherSystemError, code)
	assert.Equal(t, err.Error(), str)
}
