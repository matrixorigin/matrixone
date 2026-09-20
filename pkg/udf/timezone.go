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

package udf

import (
	"bufio"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"os"
	"strings"
)

// TimezoneDatabaseVersion returns the version of the system IANA tz database
// used by the current process. An IANA timezone is part of the Python UDF
// statement contract, so silently returning a made-up version would allow two
// hosts to interpret a DST boundary differently.
func TimezoneDatabaseVersion() (string, error) {
	paths := []string{
		"/usr/share/zoneinfo/tzdata.zi",
		"/usr/lib/zoneinfo/tzdata.zi",
		"/usr/share/lib/zoneinfo/tzdata.zi",
		"/etc/zoneinfo/tzdata.zi",
	}
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		lineOK := scanner.Scan()
		line := scanner.Text()
		_ = file.Close()
		if !lineOK {
			continue
		}
		const prefix = "# version "
		if strings.HasPrefix(line, prefix) {
			version := strings.TrimSpace(strings.TrimPrefix(line, prefix))
			if version != "" {
				return version, nil
			}
		}
	}
	return "", moerr.NewInternalErrorNoCtxf("python udf: IANA timezone database version is unavailable")
}
