//go:build gpu

/*
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cuvs

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

// Pack archives all files in dirPath into a single .tar or .tar.gz file.
// save_dir already writes manifest.json to dirPath, so it is included automatically.
// If outputPath ends with .gz, gzip compression is used.
func Pack(dirPath string, outputPath string) error {
	outFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	var tw *tar.Writer
	var gw *gzip.Writer

	if strings.HasSuffix(outputPath, ".gz") {
		gw = gzip.NewWriter(outFile)
		tw = tar.NewWriter(gw)
	} else {
		tw = tar.NewWriter(outFile)
	}

	files, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filePath := filepath.Join(dirPath, file.Name())
		fi, err := os.Stat(filePath)
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(fi, "")
		if err != nil {
			return err
		}
		header.Name = file.Name()

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		f, err := os.Open(filePath)
		if err != nil {
			return err
		}
		if _, err := io.Copy(tw, f); err != nil {
			f.Close()
			return err
		}
		f.Close()
	}

	if err := tw.Close(); err != nil {
		return err
	}
	if gw != nil {
		if err := gw.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Unpack extracts components from a .tar or .tar.gz file into a directory and returns the manifest.
func Unpack(inputPath string, dirPath string) (string, error) {
	in, err := os.Open(inputPath)
	if err != nil {
		return "", err
	}
	defer in.Close()

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return "", err
	}

	var tr *tar.Reader
	if strings.HasSuffix(inputPath, ".gz") {
		gr, err := gzip.NewReader(in)
		if err != nil {
			return "", err
		}
		defer gr.Close()
		tr = tar.NewReader(gr)
	} else {
		tr = tar.NewReader(in)
	}

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		target := filepath.Join(dirPath, header.Name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return "", err
			}
		case tar.TypeReg:
			f, err := os.Create(target)
			if err != nil {
				return "", err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return "", err
			}
			f.Close()
		}
	}

	// Read manifest.json
	manifestPath := filepath.Join(dirPath, "manifest.json")
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return "", moerr.NewInternalErrorNoCtx(fmt.Sprintf("failed to read manifest from tar: %v", err))
	}

	logutil.Infof("Unpack manifest: %s", string(manifestBytes))

	return string(manifestBytes), nil
}

// ---------------------------------------------------------------------------
// SHARDED load-time device-list sizing
//
// In Sharded mode the C++ index uses one shard per device in the supplied
// list. Build-time callers just pass the devices they want sharded over —
// len(devices) is the shard count.
//
// Load-time is asymmetric: the saved index already has a fixed shard count
// baked into manifest.json, but the caller doesn't necessarily know that
// count when picking how many devices to pass. PeekManifestNShards lets the
// FromDataDirectory wrappers size their device list to match before
// constructing the C++ index, so worker threads + RMM pools are only
// allocated on devices that will actually host a shard.
// ---------------------------------------------------------------------------

// manifestPeek is the minimal JSON shape we need to read off disk to learn
// the saved shard count. Everything else in the manifest is consumed by
// the C++ load_dir.
type manifestPeek struct {
	BuildParams struct {
		ShardSizes []uint64 `json:"shard_sizes"`
	} `json:"build_params"`
	Components struct {
		Shards []string `json:"shards"`
	} `json:"components"`
}

// PeekManifestNShards reads <dir>/manifest.json and returns the number of
// shards the saved index has. For non-SHARDED indexes the manifest has no
// shard_sizes / shards entries and this returns 0.
//
// Used by NewGpu*FromDataDirectory wrappers to size the caller-supplied
// devices list before constructing the C++ index, so worker threads and
// RMM pools are only spawned on the devices the SHARDED index actually
// uses.
func PeekManifestNShards(dir string) (uint32, error) {
	path := filepath.Join(dir, "manifest.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, moerr.NewInternalErrorNoCtx(
			fmt.Sprintf("failed to read %s: %v", path, err))
	}
	var m manifestPeek
	if err := json.Unmarshal(raw, &m); err != nil {
		return 0, moerr.NewInternalErrorNoCtx(
			fmt.Sprintf("failed to parse %s: %v", path, err))
	}
	// shard_sizes is authoritative; fall back to components.shards if missing.
	if n := len(m.BuildParams.ShardSizes); n > 0 {
		return uint32(n), nil
	}
	return uint32(len(m.Components.Shards)), nil
}

// devicesForLoad returns the slice of devices to pass to the C constructor
// when loading from disk. It peeks the manifest at `dir` to learn the saved
// shard count, then trims devices to that count for SHARDED loads. For
// non-SHARDED loads (or when the manifest has no shard count) it returns
// devices unchanged. Errors if the saved index has more shards than the
// caller supplied devices.
func devicesForLoad(devices []int, mode DistributionMode, dir string) ([]int, error) {
	if mode != Sharded {
		return devices, nil
	}
	savedN, err := PeekManifestNShards(dir)
	if err != nil {
		return nil, err
	}
	if savedN == 0 {
		return devices, nil
	}
	if int(savedN) > len(devices) {
		return nil, moerr.NewInternalErrorNoCtx(
			fmt.Sprintf("saved index has %d shards but only %d devices supplied", savedN, len(devices)))
	}
	return devices[:savedN], nil
}
