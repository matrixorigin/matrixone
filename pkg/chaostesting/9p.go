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
	"io"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/hugelgupf/p9/p9"
	"github.com/u-root/uio/ulog"
)

type Setup9P func(
	mountPoint string,
) (
	err error,
	end func() error,
)

func (_ Def) Setup9P(
	host NetworkHost,
	getPort GetPortStr,
	wt RootWaitTree,
	debug Debug9P,
) Setup9P {
	return func(
		mountPoint string,
	) (
		err error,
		end func() error,
	) {

		port := getPort(math.MaxInt)
		addr := net.JoinHostPort(string(host), port)
		config := net.ListenConfig{}
		ln, err := config.Listen(wt.Ctx, "tcp", addr)
		ce(err)

		var options []p9.ServerOpt
		if debug {
			options = append(options, p9.WithServerLogger(ulog.Log))
		}
		server := p9.NewServer(
			newP9Attacher(),
			options...,
		)

		closing := false
		go func() {
			err := server.Serve(ln)
			if closing {
				return
			}
			ce(err)
		}()

		t0 := time.Now()
		for {

			options := fmt.Sprintf("trans=tcp,port=%s,version=9p2000.L,cache=mmap", port)

			err := mount(string(host), mountPoint, "9p", options)

			if err == nil {
				break
			} else {
				//pt("%s\n", output)
				pt("%v\n", err)
				if time.Since(t0) > time.Second*1 {
					panic("p9 mount timeout")
				}
			}
		}

		end = func() error {
			ce(syscall.Unmount(mountPoint, 0))
			closing = true
			ce(ln.Close())
			return nil
		}

		return
	}
}

type Debug9P bool

func (_ Def) Debug9P() Debug9P {
	return false
}

type p9Attacher struct {
	root *p9File
}

func newP9Attacher() *p9Attacher {
	root := newP9File()
	root.mode = p9.ModeDirectory | p9.AllPermissions
	return &p9Attacher{
		root: root,
	}
}

var _ p9.Attacher = new(p9Attacher)

func (p *p9Attacher) Attach() (p9.File, error) {
	return &p9Handle{
		file: p.root,
	}, nil
}

type p9File struct {
	id      uint64
	version uint32
	mode    p9.FileMode
	content []byte
	symlink string
	atime   time.Time
	mtime   time.Time
	ctime   time.Time
	btime   time.Time

	subs []p9DirEntry
}

type p9DirEntry struct {
	name string
	file *p9File
}

func newP9File() *p9File {
	return &p9File{
		id:      uint64(rand.Int63()),
		version: 1,
		atime:   time.Now(),
		mtime:   time.Now(),
		ctime:   time.Now(),
		btime:   time.Now(),
	}
}

type p9Handle struct {
	p9.DefaultWalkGetAttr
	path string
	file *p9File
}

func (h *p9Handle) clone() *p9Handle {
	return &p9Handle{
		path: h.path,
		file: h.file,
	}
}

var _ p9.File = new(p9Handle)

func (h *p9Handle) Close() error {
	return nil
}

func (h *p9Handle) Renamed(parent p9.File, newName string) {
	h.path = filepath.Join(
		filepath.Dir(h.path),
		newName,
	)
}

func (h *p9Handle) Walk(names []string) ([]p9.QID, p9.File, error) {
	if len(names) == 0 {
		h.file.atime = time.Now()
		return []p9.QID{
			h.file.qid(),
		}, h.clone(), nil
	}

	curFile := h.file
	var qids []p9.QID
	for len(names) > 0 {
		name := names[0]
		names = names[1:]
		curFile.atime = time.Now()
		sub := curFile.find(name)
		if sub == nil {
			return qids, nil, os.ErrNotExist
		}
		sub.atime = time.Now()
		qids = append(qids, sub.qid())
		curFile = sub
	}

	return qids, &p9Handle{
		path: filepath.Join(
			h.path,
			filepath.Join(names...),
		),
		file: curFile,
	}, nil
}

func (h *p9Handle) StatFS() (p9.FSStat, error) {
	return p9.FSStat{
		BlockSize:       512,
		Blocks:          math.MaxInt,
		BlocksFree:      math.MaxInt,
		BlocksAvailable: math.MaxInt,
		Files:           math.MaxInt,
		FilesFree:       math.MaxInt,
		FSID:            42,
		NameLength:      65536,
	}, nil
}

func (h *p9Handle) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	h.file.atime = time.Now()
	return h.file.qid(), h.file.version, nil
}

func (h *p9Handle) ReadAt(buf []byte, offset int64) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	h.file.atime = time.Now()
	n := copy(buf, h.file.content[offset:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func (f *p9File) qid() p9.QID {
	return p9.QID{
		Type:    f.mode.QIDType(),
		Version: f.version,
		Path:    f.id,
	}
}

func (f *p9File) attr() p9.Attr {
	return p9.Attr{
		Mode:             f.mode,
		Size:             uint64(len(f.content)),
		BlockSize:        0,
		Blocks:           0,
		ATimeSeconds:     uint64(f.atime.Unix()),
		ATimeNanoSeconds: uint64(f.atime.UnixNano()),
		MTimeSeconds:     uint64(f.mtime.Unix()),
		MTimeNanoSeconds: uint64(f.mtime.UnixNano()),
		CTimeSeconds:     uint64(f.ctime.Unix()),
		CTimeNanoSeconds: uint64(f.ctime.UnixNano()),
		BTimeSeconds:     uint64(f.btime.Unix()),
		BTimeNanoSeconds: uint64(f.btime.UnixNano()),
	}
}

func (h *p9Handle) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	h.file.atime = time.Now()
	return h.file.qid(), req, h.file.attr(), nil
}

func (h *p9Handle) SetAttr(mask p9.SetAttrMask, attr p9.SetAttr) error {
	h.file.mtime = time.Now()
	if mask.Permissions {
		h.file.mode = h.file.mode.FileType() | attr.Permissions
	}
	if mask.ATime {
		h.file.atime = time.Unix(int64(attr.ATimeSeconds), int64(attr.ATimeNanoSeconds))
	}
	if mask.MTime {
		h.file.mtime = time.Unix(int64(attr.MTimeSeconds), int64(attr.MTimeNanoSeconds))
	}
	return nil
}

func (h *p9Handle) Rename(directory p9.File, name string) error {
	return syscall.ENOSYS
}

func (h *p9Handle) FSync() error {
	//TODO expose
	return nil
}

func (h *p9Handle) WriteAt(data []byte, offset int64) (int, error) {
	h.file.mtime = time.Now()
	if l := int(offset) + len(data); l > len(h.file.content) {
		newContent := make([]byte, l)
		copy(newContent, h.file.content)
		h.file.content = newContent
	}
	copy(h.file.content[offset:], data)
	return len(data), nil
}

func (h *p9Handle) Create(name string, mode p9.OpenFlags, permissions p9.FileMode, uid p9.UID, gid p9.GID) (p9.File, p9.QID, uint32, error) {
	if sub := h.file.find(name); sub != nil {
		return &p9Handle{
			path: filepath.Join(h.path, name),
			file: sub,
		}, sub.qid(), sub.version, nil
		//TODO check for existence?
		return nil, p9.QID{}, 0, os.ErrExist
	}

	newFile := newP9File()
	newFile.mode = p9.ModeRegular | p9.AllPermissions | permissions

	h.file.set(name, newFile)
	h.file.mtime = time.Now()

	return &p9Handle{
		path: filepath.Join(h.path, name),
		file: newFile,
	}, newFile.qid(), newFile.version, nil
}

func (h *p9Handle) Mkdir(name string, permissions p9.FileMode, uid p9.UID, gid p9.GID) (p9.QID, error) {
	if sub := h.file.find(name); sub != nil {
		return p9.QID{}, os.ErrExist
	}

	newFile := newP9File()
	newFile.mode = p9.ModeDirectory | p9.AllPermissions | permissions

	h.file.set(name, newFile)
	h.file.mtime = time.Now()

	return newFile.qid(), nil
}

func (h *p9Handle) Symlink(oldName string, newName string, _ p9.UID, _ p9.GID) (p9.QID, error) {
	if sub := h.file.find(newName); sub != nil {
		return p9.QID{}, os.ErrExist
	}

	newFile := newP9File()
	newFile.mode = p9.ModeSymlink | p9.AllPermissions
	newFile.symlink = oldName

	h.file.set(newName, newFile)
	h.file.mtime = time.Now()

	return newFile.qid(), nil
}

func (h *p9Handle) Link(target p9.File, newName string) error {
	targetHandle := target.(*p9Handle)
	targetHandle.file.set(newName, h.file)
	return nil
}

func (h *p9Handle) Mknod(name string, mode p9.FileMode, major uint32, minor uint32, _ p9.UID, _ p9.GID) (p9.QID, error) {
	return p9.QID{}, syscall.ENOSYS
}

func (h *p9Handle) RenameAt(oldName string, newDir p9.File, newName string) error {
	sub := h.file.find(oldName)
	if sub == nil {
		return os.ErrNotExist
	}
	h.file.mtime = time.Now()
	h.file.del(oldName)
	newHandle := newDir.(*p9Handle)
	newHandle.file.mtime = time.Now()
	newHandle.file.set(newName, sub)
	return nil
}

func (h *p9Handle) UnlinkAt(name string, flags uint32) error {
	h.file.mtime = time.Now()
	h.file.del(name)
	return nil
}

func (h *p9Handle) Readdir(offset uint64, count uint32) (ret p9.Dirents, err error) {
	c := int(count)
	for i := int(offset); i < len(h.file.subs); i++ {
		if len(ret) >= c {
			break
		}
		entry := h.file.subs[i]
		file := entry.file
		ret = append(ret, p9.Dirent{
			QID:    file.qid(),
			Offset: offset + uint64(i),
			Type:   file.mode.QIDType(),
			Name:   entry.name,
		})
	}
	if len(ret) < c {
		return ret, io.EOF
	}
	return
}

func (h *p9Handle) Readlink() (string, error) {
	return h.file.symlink, nil
}

func (f *p9File) find(name string) *p9File {
	i := sort.Search(len(f.subs), func(i int) bool {
		return name <= f.subs[i].name
	})
	if i < len(f.subs) && f.subs[i].name == name {
		return f.subs[i].file
	}
	return nil
}

func (f *p9File) set(name string, file *p9File) {
	i := sort.Search(len(f.subs), func(i int) bool {
		return name <= f.subs[i].name
	})
	if i < len(f.subs) {
		if f.subs[i].name == name {
			f.subs[i].file = file
		} else {
			newSubs := append(f.subs[:i], p9DirEntry{
				name: name,
				file: file,
			})
			newSubs = append(newSubs, f.subs[i:]...)
			f.subs = newSubs
		}
	} else {
		f.subs = append(f.subs, p9DirEntry{
			name: name,
			file: file,
		})
	}
}

func (f *p9File) del(name string) {
	i := sort.Search(len(f.subs), func(i int) bool {
		return name <= f.subs[i].name
	})
	if i < len(f.subs) {
		f.subs = append(f.subs[:i], f.subs[i+1:]...)
	}
}
