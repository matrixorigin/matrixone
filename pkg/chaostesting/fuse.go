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
	"context"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type inode struct {
	fs.Inode

	mu      sync.Mutex
	content []byte
	modTime time.Time
}

var _ fs.InodeEmbedder = new(inode)

var _ fs.NodeGetattrer = new(inode)

func (i *inode) Getattr(ctx context.Context, handle fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.getAttr(out)
	return 0
}

func (i *inode) getAttr(out *fuse.AttrOut) {
	out.Size = uint64(len(i.content))
	out.SetTimes(nil, &i.modTime, nil)
}

var _ fs.NodeSetattrer = new(inode)

func (i *inode) Setattr(ctx context.Context, handle fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	i.mu.Lock()
	defer i.mu.Unlock()
	if size, ok := in.GetSize(); ok {
		i.resize(size)
	}
	i.getAttr(out)
	return 0
}

func (i *inode) resize(newSize uint64) {
	if newSize > uint64(len(i.content)) {
		newContent := make([]byte, newSize)
		copy(newContent, i.content)
		i.content = newContent
	} else {
		i.content = i.content[:newSize]
	}
	i.modTime = time.Now()
}

var _ fs.NodeReader = new(inode)

func (i *inode) Read(ctx context.Context, handle fs.FileHandle, dest []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	i.mu.Lock()
	defer i.mu.Unlock()
	end := offset + int64(len(dest))
	if l := int64(len(i.content)); end > l {
		end = l
	}
	return fuse.ReadResultData(i.content[offset:end]), 0
}

var _ fs.NodeWriter = new(inode)

func (i *inode) Write(ctx context.Context, handle fs.FileHandle, buf []byte, offset int64) (uint32, syscall.Errno) {
	i.mu.Lock()
	defer i.mu.Unlock()
	size := len(buf)
	if newSize := offset + int64(size); newSize > int64(len(i.content)) {
		i.resize(uint64(newSize))
	}
	copy(i.content[offset:], buf)
	i.modTime = time.Now()
	return uint32(size), 0
}

var _ fs.NodeOpener = new(inode)

func (i *inode) Open(ctx context.Context, openFlags uint32) (handle fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return nil, 0, 0
}

var _ fs.NodeCreater = new(inode)

func (i *inode) Create(
	ctx context.Context,
	name string,
	flags uint32,
	mode uint32,
	out *fuse.EntryOut,
) (
	node *fs.Inode,
	handle fs.FileHandle,
	fuseFlags uint32,
	errno syscall.Errno,
) {
	i.mu.Lock()
	defer i.mu.Unlock()
	childNode := new(inode)
	child := i.NewInode(ctx, childNode, fs.StableAttr{
		Mode: mode,
	})
	return child, childNode, 0, 0
}

var _ fs.NodeMkdirer = new(inode)

func (i *inode) Mkdir(
	ctx context.Context,
	name string,
	mode uint32,
	out *fuse.EntryOut,
) (
	node *fs.Inode,
	errno syscall.Errno,
) {
	i.mu.Lock()
	defer i.mu.Unlock()
	childNode := new(inode)
	mode |= syscall.S_IFDIR
	child := i.NewInode(ctx, childNode, fs.StableAttr{
		Mode: mode,
	})
	attr := child.StableAttr()
	out.Attr.Mode = attr.Mode
	out.Attr.Ino = attr.Ino
	return child, 0
}

var _ fs.NodeFsyncer = new(inode)

func (i *inode) Fsync(ctx context.Context, handle fs.FileHandle, flags uint32) syscall.Errno {
	return 0
}

var _ fs.NodeRenamer = new(inode)

func (i *inode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flag uint32) syscall.Errno {
	return 0
}
