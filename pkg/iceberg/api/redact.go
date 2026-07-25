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

package api

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const RedactedValue = "<redacted>"

func RedactField(key, value string) string {
	if IsSensitiveField(key) && value != "" {
		return RedactedValue
	}
	return value
}

func IsSensitiveField(key string) bool {
	k := normalizedSensitiveKey(key)
	return strings.Contains(k, "secret") ||
		strings.Contains(k, "token") ||
		strings.Contains(k, "password") ||
		strings.Contains(k, "credential") ||
		strings.Contains(k, "authorization") ||
		strings.Contains(k, "accesskey") ||
		strings.Contains(k, "apikey") ||
		strings.Contains(k, "signature") ||
		strings.Contains(k, "bearer")
}

func normalizedSensitiveKey(key string) string {
	k := strings.ToLower(key)
	replacer := strings.NewReplacer("_", "", "-", "", ".", "", " ", "")
	return replacer.Replace(k)
}

func RedactFields(fields map[string]string) map[string]string {
	if len(fields) == 0 {
		return nil
	}
	out := make(map[string]string, len(fields))
	for k, v := range fields {
		out[k] = RedactField(k, v)
	}
	return out
}

func RedactPath(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	return "<redacted:path:" + PathHash(path) + ">"
}

func PathHash(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(path))
	return hex.EncodeToString(sum[:8])
}
