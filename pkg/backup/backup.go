package backup

import (
	"context"
	"encoding/csv"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"golang.org/x/exp/rand"
	"os"
	"path"
	"strings"
)

// Backup
// Note: ctx needs to support cancel. The user can cancel the backup task by canceling the ctx.
func Backup(ctx context.Context, bs *tree.BackupStart, cfg *Config) error {
	var err error
	var s3Conf *s3Config
	cfg.Metas = NewMetas()

	// step 1 : setup fileservice
	//1.1 setup ETL fileservice for general usage
	if !bs.IsS3 {
		cfg.GeneralDir, _, err = setupFilesystem(ctx, bs.Dir, true)
		if err != nil {
			return err
		}
		//for tae hakeeper
		cfg.TaeDir, _, err = setupFilesystem(ctx, bs.Dir, false)
		if err != nil {
			return err
		}
	} else {
		s3Conf, err = getS3Config(ctx, bs.Option)
		if err != nil {
			return err
		}
		cfg.GeneralDir, _, err = setupS3(ctx, s3Conf, true)
		if err != nil {
			return err
		}
		cfg.TaeDir, _, err = setupS3(ctx, s3Conf, false)
		if err != nil {
			return err
		}
	}

	// step 2 : backup mo
	if err = backupBuildInfo(ctx, cfg); err != nil {
		return err
	}

	if err = backupConfigs(ctx, cfg); err != nil {
		return err
	}

	if err = backupTae(ctx, cfg); err != nil {
		return err
	}

	if err = backupHakeeper(ctx, cfg); err != nil {
		return err
	}

	if err = saveMetas(ctx, cfg); err != nil {
		return err
	}

	return err
}

// saveBuildInfo saves backupVersion, build info.
func backupBuildInfo(ctx context.Context, cfg *Config) error {
	cfg.Metas.AppendVersion(Version)
	cfg.Metas.AppendBuildinfo(buildInfo())
	return nil
}

// saveConfigs saves cluster config or service config
func backupConfigs(ctx context.Context, cfg *Config) error {
	var err error
	// save cluster config files
	for typ, files := range launchConfigPaths {
		for _, f := range files {
			err = backupConfigFile(ctx, typ, f, cfg)
			if err != nil {
				return err
			}
		}
	}

	return err
}

func backupTae(ctx context.Context, config *Config) error {
	fs := fileservice.SubPath(config.TaeDir, taeDir)
	return BackupData(ctx, config.SharedFs, fs, "")
}

func backupHakeeper(ctx context.Context, config *Config) error {
	var (
		err    error
		haData []byte
	)
	if config.HAkeeper == nil {
		return moerr.NewInternalError(ctx, "hakeeper client is nil")
	}
	fs := fileservice.SubPath(config.TaeDir, hakeeperDir)
	// get hakeeper data
	haData, err = config.HAkeeper.GetBackupData(ctx)
	if err != nil {
		return err
	}
	return writeFile(ctx, fs, HakeeperFile, haData)
}

func saveDumpFiles(ctx context.Context, fs fileservice.FileService, size, count int) error {
	var err error
	data := make([]byte, size*mb)
	for i := 0; i < count; i++ {
		_, _ = rand.Read(data)
		uid, _ := uuid.NewUUID()
		err = writeFile(ctx, fs, uid.String(), data)
		if err != nil {
			return err
		}
	}
	return err
}

func saveDumpHakeeper(ctx context.Context, fs fileservice.FileService, size int) error {
	data := make([]byte, size*mb)
	_, _ = rand.Read(data)
	return writeFile(ctx, fs, HakeeperFile, data)
}

func backupConfigFile(ctx context.Context, typ, configPath string, cfg *Config) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		logutil.Errorf("read file %s failed, err: %v", configPath, err)
		//!!!neglect the error
		return nil
	}
	uid, _ := uuid.NewUUID()
	_, file := path.Split(configPath)
	newfile := file + "_" + uid.String()
	cfg.Metas.AppendLaunchconfig(typ, newfile)
	filename := configDir + "/" + newfile
	return writeFile(ctx, cfg.GeneralDir, filename, data)
}

func saveMetas(ctx context.Context, cfg *Config) error {
	lines := cfg.Metas.CsvString()
	metas, err := ToCsvLine2(lines)
	if err != nil {
		return err
	}
	return writeFile(ctx, cfg.GeneralDir, moMeta, []byte(metas))
}

func ToCsvLine2(s [][]string) (string, error) {
	ss := strings.Builder{}
	writer := csv.NewWriter(&ss)
	for _, t := range s {
		err := writer.Write(t)
		if err != nil {
			return "", err
		}
	}

	writer.Flush()
	return ss.String(), nil
}
