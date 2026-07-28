// Copyright 2021-2025 Matrix Origin
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

package client

func (tc *txnOperator) Set(key string, value any) {
	tc.reset.cache.Store(key, value)
}

func (tc *txnOperator) Get(key string) (any, bool) {
	return tc.reset.cache.Load(key)
}

func (tc *txnOperator) Delete(key string) {
	tc.reset.cache.Delete(key)
}
