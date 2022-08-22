// Copyright 2022 Matrix Origin
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

import (
	"fmt"
	"strings"
)

func splitPath(serviceName string, path string) (string, string, error) {
	parts := strings.SplitN(path, ":", 2)
	if len(parts) == 2 {
		if serviceName != "" && parts[0] != "" && parts[0] != serviceName {
			return "", "", fmt.Errorf("wrong file service name, expecting %s, got %s", serviceName, parts[0])
		}
		return parts[0], parts[1], nil
	}
	return "", parts[0], nil
}
