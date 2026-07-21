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

package testutil

import (
	"regexp"
	"strings"
	"testing"
)

var (
	icebergObjectPathRE = regexp.MustCompile(`(?i)\b(?:s3|s3a|s3n|gs|abfs|abfss|oss|cos)://[^\s"'<>),;]+`)
	icebergSignedURLRE  = regexp.MustCompile(`(?i)(?:[?&]|%3[fF]|%26)(?:X-Amz-[A-Za-z0-9-]+|AWSAccessKeyId|Signature|Expires|X-Goog-[A-Za-z0-9-]+|GoogleAccessId|X-OSS-[A-Za-z0-9-]+)=`)
)

func AssertNoIcebergSensitiveLeak(t testing.TB, label, text string, extraForbidden ...string) {
	t.Helper()
	for _, value := range append(defaultIcebergForbiddenSubstrings(), extraForbidden...) {
		if value != "" && strings.Contains(text, value) {
			t.Fatalf("%s leaked %q:\n%s", label, value, text)
		}
	}
	if match := icebergObjectPathRE.FindString(text); match != "" {
		t.Fatalf("%s leaked object path %q:\n%s", label, match, text)
	}
	if match := icebergSignedURLRE.FindString(text); match != "" {
		t.Fatalf("%s leaked signed URL query parameter %q:\n%s", label, match, text)
	}
}

func AssertHasIcebergRedactionMarker(t testing.TB, label, text string) {
	t.Helper()
	if !HasIcebergRedactionMarker(text) {
		t.Fatalf("%s expected Iceberg redaction marker:\n%s", label, text)
	}
}

func HasIcebergRedactionMarker(text string) bool {
	return strings.Contains(text, "<redacted") ||
		strings.Contains(text, `\u003credacted`)
}

func defaultIcebergForbiddenSubstrings() []string {
	return []string{
		"AKIA_EMBEDDED_TEST",
		"raw-token",
		"raw-secret",
		"raw-secret-key",
		"raw-secret-for-redaction-test",
		"session-token-for-redaction-test",
		"secret-token",
		"secret-key",
		"scope://catalog/secret-token",
		"secret://catalog/token",
		"object-scope-ref-with-secret",
	}
}
