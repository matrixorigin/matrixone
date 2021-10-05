package logstore

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	DefaultSuffix string = ".rot"
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
	Dir         string
	NamePrefix  string
	NameSuffix  string
	Checker     IRotateChecker
	file        *VersionFile
	mu          sync.RWMutex
	currVersion uint64
	history     IHistory
}

func OpenRotational(dir, prefix, suffix string, historyFactory HistoryFactory, checker IRotateChecker) (*Rotational, error) {
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
			history:    historyFactory(),
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
		history:    historyFactory(),
	}
	if len(versions) == 0 {
		return rot, nil
	}
	idx := len(versions) - 1
	rot.currVersion = versions[idx].Version + 1
	rot.file = versions[idx]

	rot.history.Extend(versions[:idx])
	return rot, nil
}

func (r *Rotational) GetHistory() IHistory {
	return r.history
}

func (r *Rotational) nextFileName() (string, uint64) {
	v := atomic.AddUint64(&r.currVersion, uint64(1))
	base := MakeVersionFile(r.NamePrefix, r.NameSuffix, v-1)
	return path.Join(r.Dir, base), v
}

func (r *Rotational) scheduleNew() error {
	name, v := r.nextFileName()
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	vf := &VersionFile{Version: v, File: f}

	r.file = vf
	return nil
}

func (r *Rotational) scheduleRotate() error {
	if err := r.file.Sync(); err != nil {
		return err
	}
	file := r.file
	r.file = nil
	r.history.Append(file)

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
	r.mu.RLock()
	defer r.mu.RUnlock()
	s := fmt.Sprintf("<Rotational>[\"%s\"](history=%s)", r.currName(), r.history.String())
	return s
}

func (r *Rotational) Write(buf []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delta := int64(len(buf))
	var rotNeeded bool
	if rotNeeded, err = r.Checker.PrepareAppend(r.file, delta); err != nil {
		return n, err
	}
	if r.file == nil {
		if err = r.scheduleNew(); err != nil {
			return n, err
		}
	}
	if rotNeeded {
		if err = r.scheduleRotate(); err != nil {
			return n, err
		}
	}
	n, err = r.file.Write(buf)
	r.file.Size += int64(n)
	return n, err
}

func (r *Rotational) Sync() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.file != nil {
		return r.file.Sync()
	}
	return nil
}

func (r *Rotational) Stat() (os.FileInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.file != nil {
		return r.file.Stat()
	}
	return nil, os.ErrInvalid
}

func (r *Rotational) Truncate(size int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file != nil {
		return r.file.Truncate(size)
	}
	return os.ErrNotExist
}

func (r *Rotational) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
