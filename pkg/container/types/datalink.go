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
)

// ParseDatalink extracts data from a string
// and returns the url parts, the `offset` and `size`.
func ParseDatalink(str string) ([]string, map[string]string, error) {
	u, err := url.Parse(str)
	if err != nil {
		return nil, nil, err
	}

	switch u.Scheme {
	case "http", "https", "stage", "s3", "file":
	default:
		return nil, nil, moerr.NewNYINoCtx("unsupported url scheme %s", u.Scheme)
	}

	urlParts := make([]string, 0)
	urlParts = append(urlParts, u.Scheme)
	urlParts = append(urlParts, u.User.String())
	urlParts = append(urlParts, u.Host)
	urlParts = append(urlParts, u.Path)

	urlParams := make(map[string]string)
	for k, v := range u.Query() {
		urlParams[k] = v[0]
	}
	return urlParts, urlParams, nil
}
