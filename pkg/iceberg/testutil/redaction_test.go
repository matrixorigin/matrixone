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

import "testing"

func TestIcebergRedactionAssertionsAcceptCleanText(t *testing.T) {
	text := `catalog=<redacted> path=<redacted:path:abcdef12> query=\u003credacted\u003e`
	AssertNoIcebergSensitiveLeak(t, "clean", text)
	AssertHasIcebergRedactionMarker(t, "clean", text)
	if !HasIcebergRedactionMarker(text) {
		t.Fatalf("expected redaction marker")
	}
}

func TestHasIcebergRedactionMarkerRejectsPlainText(t *testing.T) {
	if HasIcebergRedactionMarker("plain diagnostic without markers") {
		t.Fatalf("plain text should not contain a redaction marker")
	}
}
