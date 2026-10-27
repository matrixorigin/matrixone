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

package m0admission

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const ManifestEnv = "MO_M0_BIRTH_MANIFEST"
const LockFDEnv = "MO_M0_BIRTH_LOCK_FD"
const MarkerName = ".m0-birth-format-v1.json"
const LockName = ".m0-birth.lock"

type Marker struct {
	FormatVersion int    `json:"format_version"`
	BinarySHA256  string `json:"binary_sha256"`
	SourceSHA     string `json:"source_sha"`
	NativeSHA256  string `json:"native_sha256"`
}

type Manifest struct {
	Marker
	RunID     string            `json:"run_id"`
	Root      string            `json:"root"`
	Args      []string          `json:"args"`
	Files     map[string]string `json:"files"`
	DataRoots []string          `json:"data_roots"`
}

var admission struct {
	sync.RWMutex
	manifest *Manifest
}

func Enabled() bool {
	admission.RLock()
	defer admission.RUnlock()
	return admission.manifest != nil
}

// CheckPath confines all configured data/cache paths to the managed directory.
// Existing symlink ancestors are resolved even when the final file is new.
func CheckPath(root, path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return moerr.NewInvalidInputNoCtxf("M0 admission: path must be absolute and clean: %q", path)
	}
	candidate := path
	for {
		_, err := os.Lstat(candidate)
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return err
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return err
		}
		candidate = parent
	}
	real, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return err
	}
	suffix, err := filepath.Rel(candidate, path)
	if err != nil {
		return err
	}
	real = filepath.Join(real, suffix)
	rel, err := filepath.Rel(root, real)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return moerr.NewInvalidInputNoCtxf("M0 admission: path escapes managed root: %q", path)
	}
	return nil
}

func CheckManagedPath(path string) error {
	admission.RLock()
	defer admission.RUnlock()
	if admission.manifest == nil {
		return nil
	}
	return CheckPath(admission.manifest.Root, path)
}

// CheckDataDirectory also protects candidate directories when the feature is
// not admitted, including ordinary builds carrying this compatibility guard.
func CheckDataDirectory(path string) error {
	admission.RLock()
	manifest := admission.manifest
	if manifest != nil {
		defer admission.RUnlock()
		if err := CheckPath(manifest.Root, path); err != nil {
			return err
		}
		declared := false
		for _, root := range manifest.DataRoots {
			declared = declared || root == path
		}
		if !declared {
			return moerr.NewInvalidInputNoCtxf("M0 admission: undeclared data/cache directory: %q", path)
		}
		marker, err := readDataMarker(path)
		if err != nil {
			return err
		}
		if *marker != manifest.Marker {
			return moerr.NewInvalidInputNoCtxf("M0 admission: data-directory binary binding differs: %q", path)
		}
		return nil
	}
	admission.RUnlock()
	current, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	for {
		if _, err := os.Lstat(filepath.Join(current, MarkerName)); err == nil {
			return moerr.NewInvalidInputNoCtxf("M0 admission: candidate data requires its guarded binary")
		} else if !os.IsNotExist(err) {
			return err
		}
		if _, err := readDataMarker(current); err == nil {
			return moerr.NewInvalidInputNoCtxf("M0 admission: marked data directory requires admission")
		} else if !noDataMarker(err) {
			return err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return nil
		}
		current = parent
	}
}

func CheckConfigFiles(paths []string) error {
	admission.RLock()
	defer admission.RUnlock()
	if admission.manifest == nil {
		return nil
	}
	if len(paths) != len(admission.manifest.Files) {
		return moerr.NewInvalidInputNoCtxf("M0 admission: configuration set differs")
	}
	seen := make(map[string]bool, len(paths))
	for _, path := range paths {
		if seen[path] {
			return moerr.NewInvalidInputNoCtxf("M0 admission: repeated configuration")
		}
		seen[path] = true
		expected, ok := admission.manifest.Files[path]
		if !ok {
			return moerr.NewInvalidInputNoCtxf("M0 admission: undeclared configuration %q", path)
		}
		actual, err := fileHash(path)
		if err != nil {
			return err
		}
		if actual != expected {
			return moerr.NewInvalidInputNoCtxf("M0 admission: configuration changed: %q", path)
		}
	}
	return nil
}

// Initialize is called before any service or data directory is opened. The
// wrapper retains the exclusive lock across exec; missing/invalid admission
// cannot enable the feature. Close disables it before the process exits.
func Initialize(args []string) (func(), error) {
	path := os.Getenv(ManifestEnv)
	if path == "" {
		return func() {}, nil
	}
	if !birthBuildEnabled {
		return nil, moerr.NewInvalidInputNoCtxf("M0 admission: this binary lacks birth-fill capability")
	}
	return initializeManifest(path, args)
}

// initializeManifest owns manifest validation and admission publication. Keep
// the build-capability gate in Initialize, before any manifest is consumed.
func initializeManifest(path string, args []string) (func(), error) {
	var manifest Manifest
	if err := readJSON(path, &manifest); err != nil {
		return nil, err
	}
	validHex := func(s string, n int) bool {
		b, e := hex.DecodeString(s)
		return e == nil && len(b) == n && s == strings.ToLower(s)
	}
	if manifest.FormatVersion != 1 || !validHex(manifest.BinarySHA256, 32) || !validHex(manifest.NativeSHA256, 32) || !validHex(manifest.SourceSHA, 20) || manifest.RunID == "" || len(manifest.Files) == 0 || !reflect.DeepEqual(args, manifest.Args) {
		return nil, moerr.NewInvalidInputNoCtxf("M0 admission: invalid manifest identity or arguments")
	}
	root, err := filepath.EvalSymlinks(manifest.Root)
	if err != nil || root != manifest.Root || !filepath.IsAbs(root) {
		return nil, moerr.NewInvalidInputNoCtxf("M0 admission: invalid managed root")
	}
	cwd, err := os.Getwd()
	if err != nil || cwd != root {
		return nil, moerr.NewInvalidInputNoCtxf("M0 admission: working directory differs")
	}
	if err := CheckPath(root, path); err != nil {
		return nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	actual, err := fileHash(executable)
	if err != nil {
		return nil, err
	}
	if actual != manifest.BinarySHA256 {
		return nil, moerr.NewInvalidInputNoCtxf("M0 admission: executable identity differs")
	}
	var marker Marker
	if err := readJSON(filepath.Join(root, MarkerName), &marker); err != nil {
		return nil, err
	}
	if marker != manifest.Marker {
		return nil, moerr.NewInvalidInputNoCtxf("M0 admission: data format/binary binding differs; downgrade is forbidden")
	}
	fd, err := strconv.Atoi(os.Getenv(LockFDEnv))
	if err != nil || fd < 3 {
		return nil, moerr.NewInvalidInputNoCtxf("M0 admission: missing inherited lock")
	}
	var heldStat, lockStat syscall.Stat_t
	if err := syscall.Fstat(fd, &heldStat); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(root, LockName)
	if err := CheckPath(root, lockPath); err != nil {
		return nil, err
	}
	if err := syscall.Stat(lockPath, &lockStat); err != nil {
		return nil, err
	}
	if heldStat.Dev != lockStat.Dev || heldStat.Ino != lockStat.Ino {
		return nil, moerr.NewInvalidInputNoCtxf("M0 admission: lock identity differs")
	}
	lock, err := os.OpenFile(lockPath, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		return nil, moerr.NewInvalidInputNoCtxf("M0 admission: lifetime lock is not held")
	}
	if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
		return nil, err
	}
	if len(manifest.DataRoots) == 0 {
		return nil, moerr.NewInvalidInputNoCtxf("M0 admission: no bound data directories")
	}
	seenRoots := make(map[string]bool, len(manifest.DataRoots))
	for _, path := range manifest.DataRoots {
		if seenRoots[path] {
			return nil, moerr.NewInvalidInputNoCtxf("M0 admission: repeated data directory")
		}
		seenRoots[path] = true
		if err := CheckPath(root, path); err != nil {
			return nil, err
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, moerr.NewInvalidInputNoCtxf("M0 admission: data root is not a directory")
		}
		marker, err := readDataMarker(path)
		if err != nil {
			return nil, err
		}
		if *marker != manifest.Marker {
			return nil, moerr.NewInvalidInputNoCtxf("M0 admission: data-directory format/binary differs")
		}
	}
	for file, expected := range manifest.Files {
		if err := CheckPath(root, file); err != nil {
			return nil, err
		}
		actual, err := fileHash(file)
		if err != nil {
			return nil, err
		}
		if actual != expected {
			return nil, moerr.NewInvalidInputNoCtxf("M0 admission: config identity differs")
		}
	}
	admission.Lock()
	if admission.manifest != nil {
		admission.Unlock()
		return nil, moerr.NewInvalidInputNoCtxf("M0 admission: deployment already initialized")
	}
	admission.manifest = &manifest
	admission.Unlock()
	return func() { admission.Lock(); admission.manifest = nil; admission.Unlock() }, nil
}

func readJSON(path string, out any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return moerr.NewInvalidInputNoCtxf("M0 admission: trailing manifest data")
	}
	return nil
}
func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", moerr.NewInvalidInputNoCtxf("M0 admission: not a regular file")
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
