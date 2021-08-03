// +build amd64

package and

func andX86Asm(x []int64, y []int64, r []int64) int64

func init() {
	SelAnd = andX86Asm
}
