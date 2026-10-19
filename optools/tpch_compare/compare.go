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
	"path/filepath"
)

func comparisonReceipt(path, variant string, dataset FileID) (WorkerReceipt, error) {
	r, err := readJSON[WorkerReceipt](path)
	if err != nil {
		return r, err
	}
	if r.Version != formatVersion || r.Dataset.SHA256 != dataset.SHA256 || r.Dataset.Bytes != dataset.Bytes {
		return r, errors.New("worker dataset identity or receipt version differs")
	}
	if r.Variant != variant {
		return r, fmt.Errorf("worker variant must be %s", variant)
	}
	if r.Error != "" || r.Cleanup != "complete" || len(r.Queries) == 0 {
		return r, errors.New("failed/incomplete worker receipt")
	}
	seen := map[[2]int]bool{}
	for _, q := range r.Queries {
		key := [2]int{q.Query, q.Repetition}
		if q.Query < 1 || q.Query > 22 || q.Repetition < 0 || q.Error != "" || seen[key] {
			return r, errors.New("invalid/failed or repeated worker query receipt")
		}
		seen[key] = true
	}
	return r, nil
}

func matchingSchedules(a, b WorkerReceipt) error {
	if len(a.Queries) != len(b.Queries) {
		return errors.New("worker query schedules differ")
	}
	for i, x := range a.Queries {
		y := b.Queries[i]
		if x.Query != y.Query || x.Repetition != y.Repetition {
			return errors.New("worker query schedules differ")
		}
	}
	return nil
}

func compareReceipts(dataset, left, right, leftUnlimited, rightUnlimited string) (int, error) {
	if dataset == "" || left == "" || right == "" {
		return 0, errors.New("dataset and both qualification receipts are required")
	}
	if (leftUnlimited == "") != (rightUnlimited == "") {
		return 0, errors.New("both unlimited diagnostic receipts are required for LIMIT proof")
	}
	d, err := loadDataset(dataset, false)
	if err != nil {
		return 0, err
	}
	id, err := identify(filepath.Join(dataset, "dataset.json"))
	if err != nil {
		return 0, err
	}
	a, err := comparisonReceipt(left, "qualification", id)
	if err != nil {
		return 0, err
	}
	b, err := comparisonReceipt(right, "qualification", id)
	if err != nil {
		return 0, err
	}
	if err := matchingSchedules(a, b); err != nil {
		return 0, err
	}
	indices := map[[2]int]int{}
	for i, q := range a.Queries {
		indices[[2]int{q.Query, q.Repetition}] = i
	}
	proved := map[[2]int]bool{}
	if leftUnlimited != "" {
		fa, err := comparisonReceipt(leftUnlimited, "unlimited_diagnostic", id)
		if err != nil {
			return 0, err
		}
		fb, err := comparisonReceipt(rightUnlimited, "unlimited_diagnostic", id)
		if err != nil {
			return 0, err
		}
		if fa.Engine != a.Engine || fb.Engine != b.Engine || fa.EngineVersion != a.EngineVersion || fb.EngineVersion != b.EngineVersion || fa.Binary.SHA256 != a.Binary.SHA256 || fb.Binary.SHA256 != b.Binary.SHA256 {
			return 0, errors.New("unlimited proof engine/build differs from qualification")
		}
		if err := matchingSchedules(fa, fb); err != nil {
			return 0, err
		}
		for i, x := range fa.Queries {
			key := [2]int{x.Query, x.Repetition}
			j, ok := indices[key]
			q := d.Queries[x.Query-1]
			if !ok || q.Limit <= 0 {
				return 0, errors.New("unlimited proof must match a limited qualification query and repetition")
			}
			y := fb.Queries[i]
			unlimited := q
			unlimited.Limit = 0
			if err := compareCaptures(filepath.Dir(leftUnlimited), filepath.Dir(rightUnlimited), x.Capture, y.Capture, unlimited); err != nil {
				return 0, fmt.Errorf("unlimited results: %w", err)
			}
			if err := validateLimit(filepath.Dir(left), filepath.Dir(leftUnlimited), a.Queries[j].Capture, x.Capture, q); err != nil {
				return 0, err
			}
			if err := validateLimit(filepath.Dir(right), filepath.Dir(rightUnlimited), b.Queries[j].Capture, y.Capture, q); err != nil {
				return 0, err
			}
			proved[key] = true
		}
	}
	for i, x := range a.Queries {
		if proved[[2]int{x.Query, x.Repetition}] {
			continue
		}
		if err := compareCaptures(filepath.Dir(left), filepath.Dir(right), x.Capture, b.Queries[i].Capture, d.Queries[x.Query-1]); err != nil {
			return 0, err
		}
	}
	return len(a.Queries), nil
}
