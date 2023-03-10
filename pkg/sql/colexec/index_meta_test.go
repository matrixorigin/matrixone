package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"testing"
)

func TestBuild(t *testing.T) {
	proc := testutil.NewProcess()
	vec_name := vector.New(MO_INDEX_COLTYPE[MO_INDEX_NAME].ToType())
	err := vec_name.Append([]byte("empno"), false, proc.Mp())
	if err != nil {
		t.Fatal(err)
	}
}
