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

package vectorindex

import "github.com/matrixorigin/matrixone/pkg/logutil"

// DestroyIndexResources attempts every owned resource and returns the first
// panic so the caller can preserve its own error or propagate cleanup failure.
func DestroyIndexResources[T interface{ Destroy() error }](name string, resources []T) (firstPanic any) {
	for _, resource := range resources {
		func() {
			defer func() {
				if p := recover(); p != nil {
					logutil.Errorf("%s cleanup panic: %v", name, p)
					if firstPanic == nil {
						firstPanic = p
					}
				}
			}()
			if err := resource.Destroy(); err != nil {
				logutil.Errorf("%s cleanup failed: %v", name, err)
			}
		}()
	}
	return firstPanic
}
