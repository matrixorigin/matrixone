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

	logutil.Infof("cuvs.Pack: taring %d files from %s -> %s", len(files), dirPath, outputPath)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filePath := filepath.Join(dirPath, file.Name())
		fi, err := os.Stat(filePath)
		if err != nil {
			logutil.Errorf("cuvs.Pack: Stat FAILED for %s: %v", filePath, err)
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
			logutil.Errorf("cuvs.Pack: Open FAILED for %s: %v", filePath, err)
			return err
		}
		logutil.Infof("cuvs.Pack: taring %s (%d bytes)", file.Name(), fi.Size())
		if _, err := io.Copy(tw, f); err != nil {
			f.Close()
			logutil.Errorf("cuvs.Pack: io.Copy FAILED for %s after write: %v", file.Name(), err)
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

// PeekTarManifestNShards is the tar-file counterpart of PeekManifestNShards:
// it scans the archive at tarPath for manifest.json and returns the saved
// shard count (0 for SINGLE / REPLICATED). Used by LoadIndex paths that hold
// a tar path (not a pre-extracted dir) and need the topology before calling
// NewGpu*Empty + Unpack.
func PeekTarManifestNShards(tarPath string) (uint32, error) {
	f, err := os.Open(tarPath)
	if err != nil {
		return 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("open tar %s: %v", tarPath, err))
	}
	defer f.Close()

	var r io.Reader = f
	if strings.HasSuffix(tarPath, ".gz") || strings.HasSuffix(tarPath, ".tgz") {
		gz, gerr := gzip.NewReader(f)
		if gerr != nil {
			return 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("gzip reader %s: %v", tarPath, gerr))
		}
		defer gz.Close()
		r = gz
	}
	tr := tar.NewReader(r)
	for {
		hdr, herr := tr.Next()
		if herr == io.EOF {
			break
		}
		if herr != nil {
			return 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("tar next %s: %v", tarPath, herr))
		}
		if hdr.Name != "manifest.json" && !strings.HasSuffix(hdr.Name, "/manifest.json") {
			continue
		}
		body, rerr := io.ReadAll(tr)
		if rerr != nil {
			return 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("read manifest from %s: %v", tarPath, rerr))
		}
		var m manifestPeek
		if uerr := json.Unmarshal(body, &m); uerr != nil {
			return 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("parse manifest from %s: %v", tarPath, uerr))
		}
		if n := len(m.BuildParams.ShardSizes); n > 0 {
			return uint32(n), nil
		}
		return uint32(len(m.Components.Shards)), nil
	}
	return 0, moerr.NewInternalErrorNoCtx(fmt.Sprintf("manifest.json not found in %s", tarPath))
}

// ResolveDevicesForTarLoad returns the devices slice a SHARDED-aware LoadIndex
// should hand to NewGpu*Empty, using the tar's manifest.json as the source of
// truth for the saved shard count.
//
// Semantics:
//
//   - SINGLE / REPLICATED tar (no shards entry): returns devices unchanged,
//     shardCount=0.
//   - shardCount == len(devices): returns devices unchanged.
//   - shardCount < len(devices): trims to devices[:shardCount] so we do not
//     spawn worker threads / RMM pools on devices that will not host a shard.
//   - shardCount > len(devices):
//       * single-GPU host (len(devices) == 1): pads with copies of devices[0]
//         so all shards land on the one physical GPU. This is the
//         gpu_multi_simulation case: an index built under sim=N on a
//         single-GPU host is queried on the same single-GPU host — the
//         loader auto-detects and pads, so the operator does not need to
//         also SET gpu_multi_simulation at search time.
//       * multi-GPU host (len(devices) >= 2): ERROR. If the operator has
//         multiple real GPUs and the saved index expects more shards than
//         they exposed, that is a genuine deployment misconfig — silently
//         piling shards onto some GPUs would just tank throughput. The
//         error names the counts so the operator can pick a remedy (add
//         GPUs, or query with gpu_multi_simulation=N so SimulateDevices
//         produces a list of the right size).
func ResolveDevicesForTarLoad(devices []int, tarPath string) ([]int, uint32, error) {
	shardCount, err := PeekTarManifestNShards(tarPath)
	if err != nil {
		return devices, 0, err
	}
	if shardCount == 0 {
		return devices, 0, nil
	}
	n := int(shardCount)
	if n == len(devices) {
		return devices, shardCount, nil
	}
	if n < len(devices) {
		return devices[:n], shardCount, nil
	}
	// n > len(devices): single-GPU host is the sim path; multi-GPU is a misconfig.
	if len(devices) == 1 {
		expanded := make([]int, n)
		for i := range expanded {
			expanded[i] = devices[0]
		}
		return expanded, shardCount, nil
	}
	if len(devices) == 0 {
		return devices, shardCount, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"SHARDED index at %s needs %d devices but none are available",
			tarPath, shardCount))
	}
	return devices, shardCount, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
		"SHARDED index at %s needs %d devices (one per shard) but only %d GPUs available; "+
			"add GPUs, or query with gpu_multi_simulation=%d",
		tarPath, shardCount, len(devices), shardCount))
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
