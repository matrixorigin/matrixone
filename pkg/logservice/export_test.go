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

package logservice

import (
	"context"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
)

func WriteWALDataFileForTNTest(path string, entries []WALEntry) error {
	return writeTestWALDataFile(path, entries)
}

func ValidateAndLoadWALDataForTNTest(ctx context.Context, path string) ([][]byte, error) {
	s := &Service{runtime: runtime.DefaultRuntime()}
	walData, err := s.readWALDataFile(ctx, path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	payloads := make([][]byte, len(walData.Entries))
	for i := range walData.Entries {
		payloads[i], err = loadWALRecoveryEntryData(ctx, walData, f, i)
		if err != nil {
			return nil, err
		}
	}
	return payloads, nil
}
