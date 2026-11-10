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

package main

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
)

// validateLimit is used only after both engines' unlimited results match.
// Every preceding group is complete; only the final boundary group may be a
// proper subset. Current TPC-H LIMIT projections contain exact values only.
func validateLimit(rootLimited, rootFull string, limited, full Capture, q Query) (retErr error) {
	if q.Limit <= 0 || len(q.Order) == 0 {
		return errors.New("LIMIT membership requires an ordered limited query")
	}
	if limited.Version != formatVersion || full.Version != formatVersion || limited.Rows < 0 || full.Rows < 0 || len(limited.Columns) != len(q.Columns) || len(full.Columns) != len(q.Columns) {
		return errors.New("LIMIT capture count/schema mismatch")
	}
	for _, c := range q.Columns {
		if c.Kind == "approximate" {
			return errors.New("approximate LIMIT membership has no qualified contract")
		}
	}
	if limited.Rows != min(int64(q.Limit), full.Rows) {
		return errors.New("LIMIT result count differs from unlimited reference")
	}
	if err := limited.File.verify(rootLimited); err != nil {
		return err
	}
	if err := full.File.verify(rootFull); err != nil {
		return err
	}
	a, err := openRows(filepath.Join(rootLimited, limited.File.Path))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, a.close()) }()
	b, err := openRows(filepath.Join(rootFull, full.File.Path))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, b.close()) }()
	la, fb := peerReader{reader: a, q: q}, peerReader{reader: b, q: q}
	cmp := func(a, b Row) int {
		for i, c := range q.Columns {
			if v := cmpCell(a[i], b[i], c); v != 0 {
				return v
			}
		}
		return 0
	}
	var count int64
	for count < limited.Rows {
		got, err := la.group()
		if err != nil {
			return err
		}
		want, err := fb.group()
		if err != nil {
			return err
		}
		if compareKey(got[0], want[0], q) != 0 || int64(len(got)) != min(int64(len(want)), limited.Rows-count) {
			return errors.New("LIMIT skipped a required ordering group")
		}
		slices.SortFunc(got, cmp)
		slices.SortFunc(want, cmp)
		j := 0
		for _, row := range got {
			for j < len(want) && cmp(want[j], row) < 0 {
				j++
			}
			if j == len(want) || cmp(want[j], row) != 0 {
				return fmt.Errorf("query Q%d illegal LIMIT boundary member", q.Number)
			}
			j++
		}
		count += int64(len(got))
	}
	if _, err := la.next(); !errors.Is(err, io.EOF) {
		return errors.New("extra limited result rows")
	}
	return nil
}
