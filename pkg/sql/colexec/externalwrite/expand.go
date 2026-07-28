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

package externalwrite

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util"
)

// ExpandFilePattern expands a strftime(3) pattern into a concrete path.
//
// In addition to the standard strftime directives it supports two MatrixOne
// extensions used by writable external tables:
//
//	%nN  -> n random decimal digits   (n is a decimal count, e.g. %6N -> "492013")
//	%U   -> a freshly generated UUID
//
// t is the timestamp the pattern is evaluated against (typically the statement
// start time). Time directives are rendered in UTC, so every pipeline — local
// or on a remote CN whose host may run in a different OS time zone — expands
// the same instant to the same path. It is the caller's responsibility to make
// the pattern produce unique names across parallel writers (use %U or %nN);
// the standard directives alone are not unique.
func ExpandFilePattern(pattern string, t time.Time) (string, error) {
	t = t.UTC()
	var b strings.Builder
	runes := []rune(pattern)
	n := len(runes)

	for i := 0; i < n; i++ {
		c := runes[i]
		if c != '%' {
			b.WriteRune(c)
			continue
		}
		// c == '%' ; need at least one more rune
		if i+1 >= n {
			return "", moerr.NewBadConfigf(context.TODO(), "WRITE_FILE_PATTERN: dangling '%%' at end of pattern %q", pattern)
		}

		next := runes[i+1]

		// MatrixOne extension: %nN -> n random digits.
		if next >= '0' && next <= '9' {
			j := i + 1
			count := 0
			for j < n && runes[j] >= '0' && runes[j] <= '9' {
				count = count*10 + int(runes[j]-'0')
				j++
			}
			if j >= n || runes[j] != 'N' {
				return "", moerr.NewBadConfigf(context.TODO(), "WRITE_FILE_PATTERN: expected 'N' after digit count in %%nN, pattern %q", pattern)
			}
			if count <= 0 || count > 64 {
				return "", moerr.NewBadConfigf(context.TODO(), "WRITE_FILE_PATTERN: digit count for %%nN must be in [1,64], got %d", count)
			}
			s, err := randomDigits(count)
			if err != nil {
				return "", err
			}
			b.WriteString(s)
			i = j // skip through the 'N'
			continue
		}

		// MatrixOne extension: %U -> UUID.
		if next == 'U' {
			id, err := util.FastUuid()
			if err != nil {
				return "", err
			}
			b.WriteString(id.String())
			i++
			continue
		}

		// Standard strftime directives.
		s, ok := strftimeDirective(next, t)
		if !ok {
			return "", moerr.NewBadConfigf(context.TODO(), "WRITE_FILE_PATTERN: unsupported directive %%%c in pattern %q", next, pattern)
		}
		b.WriteString(s)
		i++
	}

	return b.String(), nil
}

// MinUniqueRandomDigits is the smallest %nN width that qualifies as a
// per-writer uniqueness directive: fewer digits (e.g. %1N with 10 outcomes)
// collide between parallel pipelines with realistic probability.
const MinUniqueRandomDigits = 6

// PatternHasUniqueDirective reports whether the pattern contains a directive
// that yields a distinct value per writer (%U, or %nN with n >=
// MinUniqueRandomDigits). Patterns without one expand to the same path in
// every parallel pipeline of a statement (and in every statement within the
// same time-directive granularity), so concurrent writers would clobber each
// other; DDL validation rejects them.
func PatternHasUniqueDirective(pattern string) bool {
	runes := []rune(pattern)
	n := len(runes)
	for i := 0; i < n-1; i++ {
		if runes[i] != '%' {
			continue
		}
		next := runes[i+1]
		if next == 'U' {
			return true
		}
		if next >= '0' && next <= '9' {
			j := i + 1
			count := 0
			for j < n && runes[j] >= '0' && runes[j] <= '9' {
				count = count*10 + int(runes[j]-'0')
				j++
			}
			if j < n && runes[j] == 'N' && count >= MinUniqueRandomDigits {
				return true
			}
		}
		i++ // skip the directive character (also handles %%)
	}
	return false
}

// strftimeDirective maps a single strftime directive to its rendered value.
// Returns ok=false for directives that are not supported.
func strftimeDirective(d rune, t time.Time) (string, bool) {
	switch d {
	case '%':
		return "%", true
	case 'Y':
		return fmt.Sprintf("%04d", t.Year()), true
	case 'y':
		return fmt.Sprintf("%02d", t.Year()%100), true
	case 'm':
		return fmt.Sprintf("%02d", int(t.Month())), true
	case 'd':
		return fmt.Sprintf("%02d", t.Day()), true
	case 'H':
		return fmt.Sprintf("%02d", t.Hour()), true
	case 'I':
		h := t.Hour() % 12
		if h == 0 {
			h = 12
		}
		return fmt.Sprintf("%02d", h), true
	case 'M':
		return fmt.Sprintf("%02d", t.Minute()), true
	case 'S':
		return fmt.Sprintf("%02d", t.Second()), true
	case 'p':
		if t.Hour() < 12 {
			return "AM", true
		}
		return "PM", true
	case 'j':
		return fmt.Sprintf("%03d", t.YearDay()), true
	case 'A':
		return t.Weekday().String(), true
	case 'a':
		return t.Weekday().String()[:3], true
	case 'B':
		return t.Month().String(), true
	case 'b':
		return t.Month().String()[:3], true
	default:
		return "", false
	}
}

// randomDigits returns a string of n random decimal digits.
func randomDigits(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	for i := range buf {
		buf[i] = '0' + (buf[i] % 10)
	}
	return string(buf), nil
}
