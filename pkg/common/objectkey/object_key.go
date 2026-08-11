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

package objectkey

import (
	"strconv"
	"strings"
)

const Separator = "#"
const encodedPrefix = Separator + Separator

// Encode joins a database and object name while preserving the historical
// db#object representation when neither component contains Separator. Keys
// that need encoding start with two separators, which cannot collide with a
// canonical historical-form key because those contain exactly one separator.
func Encode(database, object string) string {
	if !strings.Contains(database, Separator) && !strings.Contains(object, Separator) {
		return database + Separator + object
	}
	return encodedPrefix + strconv.Itoa(len(database)) + Separator + database + object
}

// Decode reverses Encode and accepts historical db#object keys.
func Decode(key string) (string, string) {
	if strings.HasPrefix(key, encodedPrefix) {
		lengthEnd := strings.Index(key[len(encodedPrefix):], Separator)
		if lengthEnd >= 0 {
			lengthEnd += len(encodedPrefix)
			databaseLength, err := strconv.Atoi(key[len(encodedPrefix):lengthEnd])
			payload := key[lengthEnd+len(Separator):]
			if err == nil && databaseLength >= 0 && databaseLength <= len(payload) {
				return payload[:databaseLength], payload[databaseLength:]
			}
		}
	}
	parts := strings.SplitN(key, Separator, 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return parts[0], ""
}
