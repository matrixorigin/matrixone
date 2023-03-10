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

package lfu

type LFU struct {
}

func NewPolicy(capacity int64) *LFU {
	return &LFU{}
}

func (l *LFU) Set(key any, value any, size int64) {
	//TODO implement me
	panic("implement me")
}

func (l *LFU) Get(key any) (value any, size int64, ok bool) {
	//TODO implement me
	panic("implement me")
}

func (l *LFU) Flush() {
	//TODO implement me
	panic("implement me")
}

func (l *LFU) Size() int64 {
	//TODO implement me
	panic("implement me")
}
