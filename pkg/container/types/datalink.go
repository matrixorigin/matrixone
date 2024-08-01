// Copyright 2024 Matrix Origin
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

package types

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
)

// ParseDatalink extracts data from a Datalink string
// and returns the Mo FS url, []int{offset,size}, fileType and error
// Mo FS url: The URL that is used by MO FS to access the file
// offsetSize: The offset and size of the file to be read
// file type:  ".txt" or ".csv"
// TODO: STAGE is in active development. Will add STAGE support in the future
// TODO: Will add PDF and DOCX support in the future
func ParseDatalink(fsPath string) (string, []int, string, error) {
	u, err := url.Parse(fsPath)
	if err != nil {
		return "", nil, "", err
	}

	var moUrl string
	// 1. get moUrl from the path
	switch u.Scheme {
	case "file":
		moUrl = strings.Join([]string{u.Host, u.Path}, "")
	default:
		return "", nil, "", moerr.NewNYINoCtx("unsupported url scheme %s", u.Scheme)
	}

	// 2. get file extension
	extension := filepath.Ext(u.Path)
	switch extension {
	case ".txt", ".csv":
	default:
		return "", nil, "", moerr.NewNYINoCtx("unsupported file type %s", extension)
	}

	// 3. get size and offset from the query
	urlParams := make(map[string]string)
	for k, v := range u.Query() {
		urlParams[strings.ToLower(k)] = strings.ToLower(v[0])
	}
	offsetSize := []int{0, -1}
	if _, ok := urlParams["offset"]; ok {
		if offsetSize[0], err = strconv.Atoi(urlParams["offset"]); err != nil {
			return "", nil, "", err
		}
	}
	if _, ok := urlParams["size"]; ok {
		if offsetSize[1], err = strconv.Atoi(urlParams["size"]); err != nil {
			return "", nil, "", err
		}
	}

	if offsetSize[0] < 0 {
		return "", nil, "", moerr.NewInternalErrorNoCtx("offset cannot be negative")
	}

	if offsetSize[1] < -1 {
		return "", nil, "", moerr.NewInternalErrorNoCtx("size cannot be less than -1")
	}

	return moUrl, offsetSize, extension, nil
}
