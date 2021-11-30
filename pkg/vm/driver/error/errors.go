// Copyright 2021 Matrix Origin
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

package error

import (
	"bytes"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/codec"
)

var (
	ErrCMDNotSupport   = errors.New("command is not support")
	ErrMarshalFailed   = errors.New("request marshal has failed")
	ErrInvalidValue    = errors.New("value is invalid")
	ErrShardNotExisted = errors.New("shard is not existed")
	ErrDispatchFailed  = errors.New("dispath raft query failed")
	ErrKeyNotExisted   = errors.New("request key is not existed")
	ErrStartupTimeout  = errors.New("driver startup timeout")
)

//ErrorResp transforms the error into []byte fomat
func ErrorResp(err error, infos ...string) []byte {
	buf := bytes.Buffer{}
	for _, info := range infos {
		buf.WriteString(info)
		buf.WriteString(",")
	}
	buf.Write(codec.String2Bytes(err.Error()))
	return buf.Bytes()
}