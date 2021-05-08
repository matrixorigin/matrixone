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
