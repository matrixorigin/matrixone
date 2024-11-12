package merge

import "syscall"

func totalMem() uint64 {
	in := new(syscall.Sysinfo_t)
	err := syscall.Sysinfo(in)
	if err != nil {
		return 0
	}
	return in.Totalram * uint64(in.Unit)
}
