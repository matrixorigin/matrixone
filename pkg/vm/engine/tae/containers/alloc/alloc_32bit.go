//go:build 386 || amd64p32 || arm || armbe || mips || mipsle || mips64p32 || mips64p32le || ppc || sparc
// +build 386 amd64p32 arm armbe mips mipsle mips64p32 mips64p32le ppc sparc

package alloc

const (
	// MaxArrayLen is a safe maximum length for slices on this architecture.
	MaxArrayLen = 1<<31 - 1
)
