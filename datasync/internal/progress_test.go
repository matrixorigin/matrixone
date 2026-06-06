package datasync

import (
	"bytes"
	"regexp"
	"testing"
)

func TestProgressLoggerPrefixesTimestamp(t *testing.T) {
	var out bytes.Buffer
	newProgressLogger(&out).Printf("datasync: export started")

	got := out.String()
	matched, err := regexp.MatchString(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} datasync: export started\n$`, got)
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Fatalf("progress line = %q, want timestamp prefix", got)
	}
}
