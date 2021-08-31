//go:build amd64
// +build amd64

package or

func orX86Asm(x []int64, y []int64, r []int64) int64

func init() {
	SelOr = orX86Asm
}
