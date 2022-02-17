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

package fz

import (
	"testing"
)

func Test9P(t *testing.T) {
	NewScope().Fork(func() TempDirModel {
		return "9p"
	}, func() IsTesting {
		return true
	}, func() NetworkModel {
		return "tun"
	}, func() Debug9P {
		return false
	}).Call(func(
		dir TempDir,
		cleanup Cleanup,
	) {
		defer cleanup()

		testFS(t, string(dir))

	})
}
