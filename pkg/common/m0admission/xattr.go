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

package m0admission

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
)

// DataXattrName lives on the actual directory inode, including a separately
// mounted file service. It is invisible to immutable-object enumeration/GC.
const DataXattrName = "user.matrixone_m0_birth_format_v1"

func readDataMarker(path string) (*Marker, error) {
	size, err := unix.Getxattr(path, DataXattrName, nil)
	if err != nil {
		return nil, err
	}
	if size == 0 || size > 1024 {
		return nil, fmt.Errorf("M0 admission: invalid data-directory marker size")
	}
	data := make([]byte, size)
	n, err := unix.Getxattr(path, DataXattrName, data)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data[:n]))
	decoder.DisallowUnknownFields()
	var marker Marker
	if err := decoder.Decode(&marker); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("M0 admission: trailing data-directory marker")
	}
	return &marker, nil
}

func noDataMarker(err error) bool {
	return os.IsNotExist(err) || errors.Is(err, missingDataXattr) || errors.Is(err, unix.ENOTSUP)
}
