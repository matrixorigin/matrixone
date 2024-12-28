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

package ckputil

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
)

// GetMaxTSOfCompactCKP returns the max ts of the compact checkpoint
func GetMaxTSOfCompactCKP(
	ctx context.Context,
	fs fileservice.FileService,
) (ts types.TS, err error) {
	var dirEntry *fileservice.DirEntry
	for dirEntry, err = range fs.List(ctx, ioutil.GetCheckpointDir()) {
		if err != nil {
			return
		}
		if dirEntry.IsDir {
			continue
		}
		if meta := ioutil.DecodeCKPMetaName(dirEntry.Name); meta.IsCompactExt() {
			if ts.LT(meta.GetEnd()) {
				ts = *meta.GetEnd()
			}
		}
	}
	return
}
