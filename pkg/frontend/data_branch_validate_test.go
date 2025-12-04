package frontend

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func newValidateSession(t *testing.T) *Session {
	t.Helper()

	proc := testutil.NewProcess(t)
	service := "validate-output-dir"

	InitServerLevelVars(service)
	setPu(service, &config.ParameterUnit{
		SV:          &config.FrontendParameters{},
		FileService: proc.Base.FileService,
	})

	return &Session{
		feSessionImpl: feSessionImpl{service: service},
		proc:          proc,
	}
}

func TestValidateOutputDirPath(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)

	t.Run("empty path", func(t *testing.T) {
		require.NoError(t, validateOutputDirPath(ctx, ses, ""))
	})

	t.Run("local directory exists", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, validateOutputDirPath(ctx, ses, dir))
	})

	t.Run("local directory missing", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "missing")
		err := validateOutputDirPath(ctx, ses, dir)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Contains(t, err.Error(), "does not exist")
	})

	t.Run("local path is file", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "file")
		require.NoError(t, err)
		require.NoError(t, f.Close())

		err = validateOutputDirPath(ctx, ses, f.Name())
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Contains(t, err.Error(), "not a directory")
	})

	t.Run("stage path through cache", func(t *testing.T) {
		stageDir := t.TempDir()
		stageURL, err := url.Parse("file://" + stageDir)
		require.NoError(t, err)
		stageName := "stage_local"

		cache := ses.proc.GetStageCache()
		cache.Set(stageName, stage.StageDef{
			Name: stageName,
			Url:  stageURL,
		})

		err = validateOutputDirPath(ctx, ses, fmt.Sprintf("stage://%s", stageName))
		require.NoError(t, err)
	})

	t.Run("shared fileservice directory exists", func(t *testing.T) {
		fs := ses.proc.Base.FileService
		dirPath := fmt.Sprintf("%s:/exists", defines.SharedFileServiceName)
		filePath := fmt.Sprintf("%s/file.txt", dirPath)

		write := fileservice.IOVector{
			FilePath: filePath,
			Entries: []fileservice.IOEntry{{
				Offset: 0,
				Size:   int64(len("x")),
				Data:   []byte("x"),
			}},
		}
		require.NoError(t, fs.Write(ctx, write))

		require.NoError(t, validateOutputDirPath(ctx, ses, dirPath))
	})

	t.Run("shared fileservice missing directory", func(t *testing.T) {
		dirPath := fmt.Sprintf("%s:/not-exist", defines.SharedFileServiceName)

		err := validateOutputDirPath(ctx, ses, dirPath)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	})

	t.Run("shared fileservice path is file", func(t *testing.T) {
		fs := ses.proc.Base.FileService
		filePath := fmt.Sprintf("%s:/just-file", defines.SharedFileServiceName)

		write := fileservice.IOVector{
			FilePath: filePath,
			Entries: []fileservice.IOEntry{{
				Offset: 0,
				Size:   int64(len("abc")),
				Data:   []byte("abc"),
			}},
		}
		require.NoError(t, fs.Write(ctx, write))

		err := validateOutputDirPath(ctx, ses, filePath)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Contains(t, err.Error(), "not a directory")
	})

	t.Run("invalid path format", func(t *testing.T) {
		err := validateOutputDirPath(ctx, ses, string([]byte{0x00, ':'}))
		require.Error(t, err)
	})

	t.Run("service argument error", func(t *testing.T) {
		err := validateOutputDirPath(ctx, ses, "s3,bad:/bucket")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	})

	t.Run("service root unreachable", func(t *testing.T) {
		err := validateOutputDirPath(ctx, ses, "s3-opts,endpoint=http://127.0.0.1:65535,region=us-east-1,bucket=b,key=k,secret=s,prefix=tmp:")
		require.Error(t, err)
	})
}
