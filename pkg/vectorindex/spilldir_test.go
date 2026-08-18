package vectorindex

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

// A nil or LOCAL-less fileservice must yield "" -- os.MkdirTemp reads that as
// $TMPDIR, so the fallback is the pre-change behaviour and no caller needs a branch.
func TestLocalSpillDirFallsBackWithoutLocalFS(t *testing.T) {
	require.Equal(t, "", LocalSpillDir(context.Background(), nil))
}

// With a LOCAL fileservice the directory is created under its root, so the tars
// land on the provisioned data volume instead of /tmp.
func TestLocalSpillDirCreatesUnderLocalRoot(t *testing.T) {
	root := t.TempDir()
	local, err := fileservice.NewLocalFS(context.Background(),
		defines.LocalFileServiceName, root, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	got := LocalSpillDir(context.Background(), local)
	require.Equal(t, filepath.Join(root, localSpillSubdir), got)

	fi, err := os.Stat(got)
	require.NoError(t, err, "the directory must be created, not just named")
	require.True(t, fi.IsDir())

	// Idempotent: a second build must not fail on an existing directory.
	require.Equal(t, got, LocalSpillDir(context.Background(), local))
}
