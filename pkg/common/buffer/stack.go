// Copyright 2021 - 2023 Matrix Origin
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

package buffer

func pop(fl *freeList) *page {
	fl.Lock()
	defer fl.Unlock()
	top := fl.head
	if top == nil {
		return nil
	}
	next := top.next
	fl.head = next
	return top
}

func push(p *page, fl *freeList) {
	fl.Lock()
	defer fl.Unlock()
	top := fl.head
	p.next = top
	fl.head = p
}
