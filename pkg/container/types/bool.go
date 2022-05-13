package types

import (
	"fmt"
	"go/constant"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func ParseValueToBool(num *tree.NumVal) (bool, error) {
	val := num.Value
	str := num.String()
	if !num.Negative() {
		v, _ := constant.Uint64Val(val)
		if v == 0 {
			return false, nil
		} else if v == 1 {
			return true, nil
		}
	}
	return false, errors.New(errno.IndeterminateDatatype, fmt.Sprintf("unsupport value: %v", str))
}