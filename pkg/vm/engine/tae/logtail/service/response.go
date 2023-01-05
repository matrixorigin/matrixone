// Copyright 2021 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
)

type LogtailResponse struct {
	logtail.LogtailResponse
}

var _ morpc.Message = (*LogtailResponse)(nil)

func (r *LogtailResponse) SetID(id uint64) {
	r.ResponseId = id
}

func (r *LogtailResponse) GetID() uint64 {
	return r.ResponseId
}

func (r *LogtailResponse) DebugString() string {
	return r.LogtailResponse.String()
}

func (r *LogtailResponse) Size() int {
	return r.ProtoSize()
}
