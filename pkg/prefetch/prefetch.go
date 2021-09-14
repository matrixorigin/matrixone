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

package prefetch

import "syscall"

const (
	SYS_read     = 0
	SYS_prefetch = 187
)

func Open(name string) (int, error) {
	return syscall.Open(name, syscall.O_RDONLY, 0)
}

func Read(fd uintptr, ptr uintptr, cnt uintptr) error {
	var err error

	_, _, en := syscall.RawSyscall(SYS_read, fd, ptr, cnt)
	if en != 0 {
		err = en
	}
	return err
}

func Prefetch(fd uintptr, off uintptr, cnt uintptr) error {
	var err error

	_, _, en := syscall.RawSyscall(SYS_prefetch, fd, off, cnt)
	if en != 0 {
		err = en
	}
	return err
}
