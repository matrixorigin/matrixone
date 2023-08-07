package compatibility

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	PrepareDir = "prepare"
)

func GetPrepareDirName() string {
	return filepath.Join("/tmp", "tae-compatibility-test", PrepareDir)
}

func GetPrepareVersionName() string {
	return filepath.Join(GetPrepareDirName(), "version")
}

func RemovePrepareDir() (string, error) {
	dir := GetPrepareDirName()
	return dir, os.RemoveAll(dir)
}

func InitPrepareEnv() error {
	dir, _ := RemovePrepareDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	file, err := os.OpenFile(
		GetPrepareVersionName(),
		os.O_RDWR|os.O_CREATE,
		os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()
	ver := int64(version)
	buf := types.EncodeInt64(&ver)
	_, err = file.Write(buf)
	return err
}

func EnsurePrepareEnvOK() error {
	versionFileName := GetPrepareVersionName()
	if _, err := os.Stat(versionFileName); err != nil {
		return err
	}
	file, err := os.OpenFile(
		versionFileName,
		os.O_RDONLY,
		os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()
	buf := make([]byte, 8)
	if _, err = file.Read(buf); err != nil {
		return err
	}
	ver := int(types.DecodeInt64(buf))
	if ver > version {
		return fmt.Errorf("prepare env version is %d, but current version is %d", ver, version)
	}

	return nil
}

func InitPrepareDirByType(typ int) (string, error) {
	dir := filepath.Join("/tmp", "tae-compatibility-test", PrepareDir, fmt.Sprintf("p%d", typ))
	os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	return dir, nil
}

func GetExecureDirName() string {
	return filepath.Join("/tmp", "tae-compatibility-test", "execute")
}

func RemoveExecuteDir() (string, error) {
	dir := GetExecureDirName()
	return dir, os.RemoveAll(dir)
}

func InitExecuteEnv() error {
	dir, _ := RemoveExecuteDir()
	return os.MkdirAll(dir, 0755)
}

func InitTestCaseExecuteDir(name string) (string, error) {
	dir := filepath.Join(GetExecureDirName(), name)
	os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	return dir, nil
}
