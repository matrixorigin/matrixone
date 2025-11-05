package idxcron

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/stretchr/testify/require"
)

func TestResolveVariableFunc(t *testing.T) {
	jstr := `{"cfg":{"kmeans_train_percent":{"t":"I", "v":10}, 
	"kmeans_max_iteration":{"t":"I", "v":4}, 
	"ivf_threads_build":{"t":"I", "v":23},
	"action":{"t":"S", "v":"action string"},
	"float":{"t":"F", "v":23.3}
	}, "action": "xxx"}`
	bj, err := bytejson.ParseFromString(jstr)
	require.Nil(t, err)

	bytes, err := bj.Marshal()
	require.Nil(t, err)

	f := getResolveVariableFuncFromMetadata(bytes)

	v1, err := f("kmeans_train_percent", false, false)
	require.Nil(t, err)
	require.Equal(t, v1, any(int64(10)))

	v2, err := f("kmeans_max_iteration", false, false)
	require.Nil(t, err)
	require.Equal(t, v2, any(int64(4)))

	v3, err := f("ivf_threads_build", false, false)
	require.Nil(t, err)
	require.Equal(t, v3, any(int64(23)))

	v4, err := f("float", false, false)
	require.Nil(t, err)
	require.Equal(t, v4, any(float64(23.3)))

	v5, err := f("action", false, false)
	require.Nil(t, err)
	require.Equal(t, v5, any("action string"))
}
