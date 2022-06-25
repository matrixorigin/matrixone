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

package logstore

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

var (
	DefaultSuffix              string = ".rot"
	DefaultRotationFileMaxSize        = 100 * int(common.M)
)

func MakeVersionFile(prefix, suffix string, version uint64) string {
	return fmt.Sprintf("%s-%d%s", prefix, version, suffix)
}

func ParseVersion(name, prefix, suffix string) (uint64, error) {
	woPrefix := strings.TrimPrefix(name, prefix+"-")
	if len(woPrefix) == len(name) {
		return 0, errors.New("parse version error")
	}
	strVersion := strings.TrimSuffix(woPrefix, suffix)
	if len(strVersion) == len(woPrefix) {
		return 0, errors.New("parse version error")
	}
	v, err := strconv.Atoi(strVersion)
	if err != nil {
		return 0, err
	}
	return uint64(v), nil
}

type Rotational struct {
	sync.RWMutex
	Dir         string
	NamePrefix  string
	NameSuffix  string
	Checker     IRotateChecker
	file        *VersionFile
	currVersion uint64
	history     IHistory
	observer    Observer
	archived    *archivedHub
	currInfo    *versionInfo
}

func OpenRotational(dir, prefix, suffix string, historyFactory HistoryFactory, checker IRotateChecker, observer Observer) (*Rotational, error) {
	if checker == nil {
		checker = &noRotationChecker{}
		// checker = &MaxSizeRotationChecker{
		// 	MaxSize: DefaultRotationFileMaxSize,
		// }
	}
	if observer == nil {
		observer = defaultObserver
	}
	if historyFactory == nil {
		historyFactory = DefaltHistoryFactory
	}
	newDir := false
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
		newDir = true
	}

	if newDir {
		rot := &Rotational{
			Dir:        dir,
			NamePrefix: prefix,
			NameSuffix: suffix,
			Checker:    checker,
			observer:   observer,
			history:    historyFactory(),
		}
		rot.archived = newArchivedHub(rot.history)
		if err := rot.scheduleNew(); err != nil {
			return nil, err
		}
		return rot, nil
	}
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	versions := make([]*VersionFile, 0)
	for _, f := range files {
		version, err := ParseVersion(f.Name(), prefix, suffix)
		if err != nil {
			continue
		}
		file, err := os.OpenFile(path.Join(dir, f.Name()), os.O_RDWR|os.O_APPEND, os.ModePerm)
		if err != nil {
			return nil, err
		}
		versions = append(versions, &VersionFile{
			File:    file,
			Version: version,
			Size:    f.Size(),
		})
	}
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].Version < versions[j].Version
	})
	rot := &Rotational{
		Dir:        dir,
		NamePrefix: prefix,
		NameSuffix: suffix,
		Checker:    checker,
		observer:   observer,
		history:    historyFactory(),
	}
	rot.archived = newArchivedHub(rot.history)
	if len(versions) == 0 {
		if err := rot.scheduleNew(); err != nil {
			return nil, err
		}
		return rot, nil
	}
	idx := len(versions) - 1
	rot.currVersion = versions[idx].Version + 1
	rot.file = versions[idx]

	rot.history.Extend(versions[:idx])
	return rot, nil
}

func (r *Rotational) OnReplayCommit(id uint64) {
	// logutil.Infof("Replay Commit %d, version %d", id, r.currInfo.id)
	if err := r.currInfo.AppendCommit(id); err != nil {
		logutil.Infof("%s, %d", r.currInfo.commit.String(), id)
		panic(err)
	}
}

func (r *Rotational) ApplyCheckpoint(rng common.Range) {
	err := r.currInfo.UnionCheckpointRange(rng)
	if err != nil {
		logutil.Warnf("%v", err)
	}
}

func (r *Rotational) ApplyCommit(id uint64) {
	err := r.currInfo.AppendCommit(id)
	if err != nil {
		panic(err)
	}
}

func (r *Rotational) OnReplayCheckpoint(rng common.Range) {
	logutil.Infof("Replay Checkpoint %s, version %d", rng.String(), r.currInfo.id)
	err := r.currInfo.UnionCheckpointRange(rng)
	if err != nil {
		logutil.Warnf("%v", err)
	}
}

func (r *Rotational) OnNewVersion(id uint64) {
	if r.currInfo != nil {
		r.currInfo.Archive()
	}
	logutil.Infof("New version %d", id)
	r.currInfo = newVersionInfo(r.archived, id)
}

func (r *Rotational) TryCompact() {
	err := r.archived.TryTruncate(nil)
	if err != nil {
		logutil.Warnf("%v", err)
	}
}

func (r *Rotational) ReplayVersions(handler VersionReplayHandler) error {
	if err := r.ReplayHistoryVersion(handler); err != nil {
		return err
	}
	r.OnNewVersion(r.file.Version)
	for {
		if err := handler(r.file, r); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}
	return nil
}

func (r *Rotational) ReplayHistoryVersion(handler VersionReplayHandler) error {
	return r.history.ReplayVersions(handler, r)
}

func (r *Rotational) GetHistory() IHistory {
	return r.history
}

func (r *Rotational) nextFileName() (string, uint64) {
	v := atomic.AddUint64(&r.currVersion, uint64(1))
	base := MakeVersionFile(r.NamePrefix, r.NameSuffix, v-1)
	return path.Join(r.Dir, base), v - 1
}

func (r *Rotational) scheduleNew() error {
	name, v := r.nextFileName()
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	vf := &VersionFile{Version: v, File: f}

	r.file = vf
	r.OnNewVersion(r.file.Version)
	return nil
}

func (r *Rotational) scheduleRotate() error {
	if err := r.syncLocked(); err != nil {
		return err
	}
	file := r.file
	r.file = nil
	r.history.Append(file)
	if r.currInfo != nil {
		logutil.Infof("%s rotated, commit range: %s, checkpoint range: %s", file.Name(), r.currInfo.commit.String(), r.currInfo.checkpoint.String())
	}
	if r.observer != nil {
		r.observer.OnRotated(file)
	}

	return r.scheduleNew()
}

func (r *Rotational) GetNextVersion() uint64 {
	return atomic.LoadUint64(&r.currVersion)
}

func (r *Rotational) currName() string {
	if r.file == nil {
		return ""
	}
	return r.file.Name()
}

func (r *Rotational) String() string {
	r.RLock()
	defer r.RUnlock()
	s := fmt.Sprintf("<Rotational>[\"%s\"](history=%s)", r.currName(), r.history.String())
	return s
}

func (r *Rotational) PrepareWrite(size int) error {
	var rotNeeded bool
	var err error
	if rotNeeded, err = r.Checker.PrepareAppend(r.file, int64(size)); err != nil {
		return err
	}
	if r.file == nil {
		if err := r.scheduleNew(); err != nil {
			return err
		}
	}
	if rotNeeded {
		if err := r.scheduleRotate(); err != nil {
			return err
		}
	}
	return nil
}

func (r *Rotational) Write(buf []byte) (n int, err error) {
	n, err = r.file.Write(buf)
	r.file.Size += int64(n)
	return n, err
}

func (r *Rotational) syncLocked() error {
	if r.file != nil {
		err := r.file.Sync()
		if r.observer != nil {
			r.observer.OnSynced()
		}
		return err
	}
	return nil
}

func (r *Rotational) Sync() error {
	r.RLock()
	defer r.RUnlock()
	return r.syncLocked()
}

func (r *Rotational) Stat() (os.FileInfo, error) {
	r.RLock()
	defer r.RUnlock()
	if r.file != nil {
		return r.file.Stat()
	}
	return nil, os.ErrInvalid
}

func (r *Rotational) Truncate(size int64) error {
	r.Lock()
	defer r.Unlock()
	if r.file != nil {
		return r.file.Truncate(size)
	}
	return os.ErrNotExist
}

func (r *Rotational) Close() error {
	r.Lock()
	defer r.Unlock()
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
