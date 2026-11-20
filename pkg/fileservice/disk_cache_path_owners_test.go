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

package fileservice

import "context"

func (d *DiskCache) startUpdate(path string) func() {
	o, _ := d.paths.acquire(context.Background(), path, nil)
	return func() { _ = o.release() }
}

func (d *DiskCache) startUpdateWithCleanup(path string, cleanup func() error) func() error {
	o, _ := d.paths.acquire(context.Background(), path, cleanup)
	return o.release
}

func (d *DiskCache) startUpdateContext(ctx context.Context, path string) (func(), error) {
	o, err := d.paths.acquire(ctx, path, nil)
	if err != nil {
		return nil, err
	}
	return func() { _ = o.release() }, nil
}

func (d *DiskCache) isUpdating(path string) bool {
	d.paths.L.Lock()
	defer d.paths.L.Unlock()
	return d.paths.owners[path] != nil
}
