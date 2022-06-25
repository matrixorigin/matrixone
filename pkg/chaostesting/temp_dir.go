// Copyright 2021 Matrix Origin
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

package fz

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type GetTempDir func() string

type CleanTempDir func(
	threshold time.Duration,
)

func (_ Def) TempDir(
	logger Logger,
	model TempDirModel,
	setupFuse SetupFuse,
	setup9P Setup9P,
) (
	get GetTempDir,
	cleanup Cleanup,
	cleanDir CleanTempDir,
) {

	var once sync.Once
	var dir string
	var cleanFuncs []func()
	cleanup = func() {
		for _, fn := range cleanFuncs {
			fn()
		}
	}

	switch model {

	case "os":

		cleanDir = func(threshold time.Duration) {
			dirs, err := filepath.Glob(filepath.Join(os.TempDir(), "testcube-*"))
			ce(err)
			for _, dir := range dirs {
				stat, err := os.Stat(dir)
				if err != nil {
					continue
				}
				modTime := stat.ModTime()
				// assuming no test can be running for longer than threshold
				if time.Since(modTime) > threshold {
					if err := os.RemoveAll(dir); err == nil {
						pt("removed temp dir: %s\n", dir)
					}
				}
			}
		}

		get = func() string {
			once.Do(func() {
				d, err := os.MkdirTemp(os.TempDir(), "testcube-*")
				ce(err)
				cleanFuncs = append(cleanFuncs, func() {
					logger.Info("remove temp dir")
					os.RemoveAll(dir)
				})
				dir = d
			})
			return dir
		}

	case "fuse":

		get = func() string {
			once.Do(func() {
				d, err := os.MkdirTemp(os.TempDir(), "testcube-*")
				ce(err)
				err, end := setupFuse(d)
				ce(err)
				cleanFuncs = append(cleanFuncs, func() {
					ce(end())
					ce(os.RemoveAll(d))
				})
				dir = d
			})
			return dir
		}

		cleanDir = func(time.Duration) {}

	case "9p":

		get = func() string {
			once.Do(func() {
				d, err := os.MkdirTemp(os.TempDir(), "testcube-*")
				ce(err)
				err, end := setup9P(d)
				ce(err)
				cleanFuncs = append(cleanFuncs, func() {
					ce(end())
					ce(os.RemoveAll(d))
				})
				dir = d
			})
			return dir
		}

		cleanDir = func(time.Duration) {}

	default:
		panic(fmt.Errorf("unknown model: %s", model))

	}

	return
}

type TempDirModel string

func (_ Def) TempDirModel() TempDirModel {
	return "os"
}
