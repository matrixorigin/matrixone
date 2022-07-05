//go:build amd64 || arm64 || arm64be || ppc64 || ppc64le || mips64 || mips64le || s390x || sparc64
// +build amd64 arm64 arm64be ppc64 ppc64le mips64 mips64le s390x sparc64

package alloc

const (
	// MaxArrayLen is a safe maximum length for slices on this architecture.
	MaxArrayLen = 1<<50 - 1
)
