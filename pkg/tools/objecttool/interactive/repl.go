// Copyright 2021 - 2026 Matrix Origin
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

package interactive

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// Run runs the interactive interface against a local DISK (CRC-framed) data dir.
func Run(path string) error {
	return RunWithKind(path, objectio.OfflineKindLocal)
}

// RunWithKind runs the interactive interface, reading the data dir in the given
// offline fs kind ("local", "local2" or "s3").
func RunWithKind(path string, kind string) error {
	return RunUnified(context.Background(), path, &ViewOptions{Kind: kind})
}
