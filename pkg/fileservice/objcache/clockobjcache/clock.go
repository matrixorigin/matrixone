// Copyright 2023 Matrix Origin
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

package clockobjcache

// TODO: Will be implemented in a later PR : https://github.com/matrixorigin/matrixone/issues/8173

type Clock struct {
}

func New(capacity int64) *Clock {
	return &Clock{}
}

func (c *Clock) Set(key any, value []byte, size int64, preloading bool) {
	//TODO implement me
	panic("implement me")
}

func (c *Clock) Get(key any, preloading bool) (value []byte, size int64, ok bool) {
	//TODO implement me
	panic("implement me")
}

func (c *Clock) Flush() {
	//TODO implement me
	panic("implement me")
}

func (c *Clock) Capacity() int64 {
	//TODO implement me
	panic("implement me")
}

func (c *Clock) Used() int64 {
	//TODO implement me
	panic("implement me")
}

func (c *Clock) Available() int64 {
	//TODO implement me
	panic("implement me")
}
