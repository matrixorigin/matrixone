package frontend

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_PathExists(t *testing.T) {
	cases := [...]struct {
		path string
		exist bool
		isfile bool
		noerr bool
	}{
		{"test/file",true,true,true},
		{"test/file-no",false,false,false},
		{"test/dir",true,false,true},
		{"test/dir-no",false,false,false},
		{"testx",false,false,false},
	}

	for _,c := range cases{
		exist,isfile,err := PathExists(c.path)
		require.True(t, (err == nil) == c.noerr)
		require.True(t, exist == c.exist)
		require.True(t, isfile == c.isfile)
	}
}