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

package kv

import (
	"matrixone/pkg/prefetch"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
	"os"
	"path"
	"syscall"
)

func New(name string) (*KV, error) {
	if _, err := os.Stat(name); os.IsNotExist(err) {
		if err := os.Mkdir(name, os.FileMode(0775)); err != nil {
			return nil, err
		}
	}
	return &KV{name}, nil
}

func (a *KV) Close() error {
	return nil
}

func (a *KV) Del(k string) error {
	return os.Remove(path.Join(a.name, k))
}

func (a *KV) Set(k string, v []byte) error {
	return os.WriteFile(path.Join(a.name, k), v, os.FileMode(0666))
}

func (a *KV) Size(k string) (int64, error) {
	var st syscall.Stat_t

	fd, err := syscall.Open(path.Join(a.name, k), syscall.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	defer syscall.Close(fd)
	if err := syscall.Fstat(fd, &st); err != nil {
		return 0, err
	}
	return st.Size, nil
}

func (a *KV) Prefetch(k string, size int64) error {
	fd, err := syscall.Open(path.Join(a.name, k), syscall.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer syscall.Close(fd)
	return prefetch.Prefetch(uintptr(fd), 0, uintptr(size))
}

func (a *KV) GetCopy(k string) ([]byte, error) {
	v, err := os.ReadFile(path.Join(a.name, k))
	if os.IsNotExist(err) {
		err = nil
	}
	return v, err
}

func (a *KV) Get(k string, size int64, proc *process.Process) ([]byte, error) {
	fd, err := syscall.Open(path.Join(a.name, k), syscall.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer syscall.Close(fd)
	data, err := proc.Alloc(size)
	if err != nil {
		return nil, err
	}
	data = data[:mempool.CountSize+size]
	if _, err := syscall.Read(fd, data[mempool.CountSize:]); err != nil {
		proc.Free(data)
		return nil, err
	}
	return data, nil
}
