//go:build !gpu

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

// No cuVS in this build, so there is no device arena to budget: nothing can ever charge device
// bytes. Zero leaves it unset and enforce skips it, which is exactly right -- naming a number
// would describe hardware this binary cannot use.
func automaticDeviceLimit() (int64, error) { return 0, nil }
