// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"context"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"github.com/prashantv/gostub"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ModuleName = "Backup"
)

func TestBackupData(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOptsAndQuickGC(nil)
	db := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer db.Close()
	defer opts.Fs.Close()

	schema := catalog.MockSchemaAll(13, 3)
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 10
	db.BindSchema(schema)
	{
		txn, err := db.DB.StartTxn(nil)
		require.NoError(t, err)
		dbH, err := testutil.CreateDatabase2(ctx, txn, "db")
		require.NoError(t, err)
		_, err = testutil.CreateRelation2(ctx, txn, dbH, schema)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctx))
	}

	totalRows := uint64(schema.Extra.BlockMaxRows * 30)
	bat := catalog.MockBatch(schema, int(totalRows))
	defer bat.Close()
	bats := bat.Split(100)

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(80)
	defer pool.Release()

	start := time.Now()
	for _, data := range bats {
		wg.Add(1)
		err := pool.Submit(testutil.AppendClosure(t, data, schema.Name, db.DB, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()
	t.Logf("Append %d rows takes: %s", totalRows, time.Since(start))

	deletedRows := 0
	{
		txn, rel := testutil.GetDefaultRelation(t, db.DB, schema.Name)
		testutil.CheckAllColRowsByScan(t, rel, int(totalRows), false)

		obj := testutil.GetOneObject(rel)
		id := obj.GetMeta().(*catalog.ObjectEntry).AsCommonID()
		err := rel.RangeDelete(id, 0, 0, handle.DT_Normal)
		require.NoError(t, err)
		deletedRows = 1
		testutil.CompactBlocks(t, 0, db.DB, "db", schema, false)

		assert.NoError(t, txn.Commit(context.Background()))
	}
	t.Log(db.Catalog.SimplePPString(common.PPL1))

	dir := path.Join(db.Dir, "/local")
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	defer service.Close()
	for _, data := range bats {
		txn, rel := db.GetRelation()
		v := testutil.GetSingleSortKeyValue(data, schema, 2)
		filter := handle.NewEQFilter(v)
		err := rel.DeleteByFilter(context.Background(), filter)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	backupTime := time.Now().UTC()
	currTs := types.BuildTS(backupTime.UnixNano(), 0)
	locations := make([]string, 0)
	locations = append(locations, backupTime.Format(time.DateTime))
	location, err := db.ForceCheckpointForBackup(ctx, currTs, 20*time.Second)
	assert.Nil(t, err)
	db.BGCheckpointRunner.DisableCheckpoint()
	locations = append(locations, location)
	checkpoints := db.BGCheckpointRunner.GetAllCheckpoints()
	files := make(map[string]string, 0)
	for _, candidate := range checkpoints {
		if files[candidate.GetLocation().Name().String()] == "" {
			var loc string
			loc = candidate.GetLocation().String()
			loc += ":"
			loc += fmt.Sprintf("%d", candidate.GetVersion())
			files[candidate.GetLocation().Name().String()] = loc
		}
	}
	for _, location := range files {
		locations = append(locations, location)
	}
	err = execBackup(ctx, "", db.Opts.Fs, service, locations, 1, types.TS{}, "full")
	assert.Nil(t, err)
	db.Opts.Fs = service
	db.Restart(ctx)
	txn, rel := testutil.GetDefaultRelation(t, db.DB, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, int(totalRows-100)-deletedRows, true)
	assert.NoError(t, txn.Commit(context.Background()))
}

func Test_saveTaeFilesList(t *testing.T) {
	type args struct {
		ctx        context.Context
		Fs         fileservice.FileService
		taeFiles   []*taeFile
		backupTime string
	}

	Fs := getTestFs(t, true)
	Fs2 := getTestFs(t, true)
	ts := time.Now().Format(time.DateTime)
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx:        context.Background(),
				Fs:         Fs,
				taeFiles:   nil,
				backupTime: "",
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return true
			},
		},
		{
			name: "t2",
			args: args{
				ctx:        context.Background(),
				Fs:         Fs,
				taeFiles:   nil,
				backupTime: ts,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				//check file
				check, err2 := readFileAndCheck(context.Background(), Fs, taeList)
				assert.NoError(t, err2)
				assert.Equal(t, check, []byte(""))
				check, err2 = readFileAndCheck(context.Background(), Fs, taeSum)
				assert.NoError(t, err2)
				lines, err2 := fromCsvBytes(check)
				assert.NoError(t, err2)
				assert.Equal(t, lines[0][0], ts)
				assert.Equal(t, lines[0][1], "0")
				return false
			},
		},
		{
			name: "t3",
			args: args{
				ctx:        context.Background(),
				Fs:         nil,
				taeFiles:   nil,
				backupTime: "",
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return true
			},
		},
		{
			name: "t4",
			args: args{
				ctx: context.Background(),
				Fs:  Fs2,
				taeFiles: []*taeFile{
					{
						path:     "t1",
						size:     1,
						checksum: []byte{1},
					},
				},
				backupTime: ts,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				//check file
				check, err2 := readFileAndCheck(context.Background(), Fs2, taeList)
				assert.NoError(t, err2)
				lines, err2 := fromCsvBytes(check)
				assert.NoError(t, err2)
				assert.Equal(t, lines[0][0], "t1")
				assert.Equal(t, lines[0][1], "1")
				assert.Equal(t, lines[0][2], hexStr([]byte{1}))
				check, err2 = readFileAndCheck(context.Background(), Fs2, taeSum)
				assert.NoError(t, err2)
				lines, err2 = fromCsvBytes(check)
				assert.NoError(t, err2)
				assert.Equal(t, lines[0][0], ts)
				assert.Equal(t, lines[0][1], "1")
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, saveTaeFilesList(tt.args.ctx, tt.args.Fs, tt.args.taeFiles, tt.args.backupTime, tt.args.backupTime, ""), fmt.Sprintf("saveTaeFilesList(%v, %v, %v, %v)", tt.args.ctx, tt.args.Fs, tt.args.taeFiles, tt.args.backupTime))
		})
	}
}

func Test_saveMetas(t *testing.T) {
	type args struct {
		ctx context.Context
		cfg *Config
	}

	Fs := getTestFs(t, true)

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx: context.Background(),
				cfg: nil,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return false
			},
		},
		{
			name: "t2",
			args: args{
				ctx: context.Background(),
				cfg: &Config{
					Timestamp:  types.TS{},
					GeneralDir: Fs,
					SharedFs:   nil,
					TaeDir:     nil,
					HAkeeper:   nil,
					Metas: &Metas{
						metas: []*Meta{
							{
								Typ:     TypeVersion,
								Version: "version",
							},
							{
								Typ:       TypeBuildinfo,
								Buildinfo: "build_info",
							},
							{
								Typ:              TypeLaunchconfig,
								LaunchConfigFile: "launch_conf",
							},
							{
								Typ:              TypeLaunchconfig,
								SubTyp:           CnConfig,
								LaunchConfigFile: "launch_cn_conf",
							},
						},
					},
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				check, err2 := readFileAndCheck(context.Background(), Fs, moMeta)
				assert.NoError(t, err2)
				lines, err2 := fromCsvBytes(check)
				assert.NoError(t, err2)
				assert.Equal(t, lines[0][0], "version")
				assert.Equal(t, lines[0][1], "version")
				assert.Equal(t, lines[1][0], "buildinfo")
				assert.Equal(t, lines[1][1], "build_info")
				assert.Equal(t, lines[2][0], "launchconfig")
				assert.Equal(t, lines[2][1], "")
				assert.Equal(t, lines[2][2], "launch_conf")
				assert.Equal(t, lines[3][0], "launchconfig")
				assert.Equal(t, lines[3][1], CnConfig)
				assert.Equal(t, lines[3][2], "launch_cn_conf")
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, saveMetas(tt.args.ctx, tt.args.cfg), fmt.Sprintf("saveMetas(%v, %v)", tt.args.ctx, tt.args.cfg))
		})
	}
}

func Test_backupConfigFile(t *testing.T) {
	type args struct {
		ctx        context.Context
		typ        string
		configPath string
		cfg        *Config
	}

	Fs := getTestFs(t, true)
	file := getTempFile(t, "", "t1", "test_t1")

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx:        context.Background(),
				typ:        "",
				configPath: "",
				cfg:        nil,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return true
			},
		},
		{
			name: "t2",
			args: args{
				ctx:        context.Background(),
				typ:        CnConfig,
				configPath: file.Name(),
				cfg: &Config{
					Timestamp:  types.TS{},
					GeneralDir: Fs,
					SharedFs:   nil,
					TaeDir:     nil,
					HAkeeper:   nil,
					Metas:      &Metas{},
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				list, err2 := Fs.List(context.Background(), configDir)
				assert.NoError(t, err2)
				var configFile string
				for _, entry := range list {
					if entry.IsDir {
						continue
					}
					configFile = entry.Name
					break
				}
				check, err2 := readFileAndCheck(context.Background(), Fs, configDir+"/"+configFile)
				assert.NoError(t, err2)
				assert.Equal(t, check, []byte("test_t1"))
				return true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, backupConfigFile(tt.args.ctx, tt.args.typ, tt.args.configPath, tt.args.cfg), fmt.Sprintf("backupConfigFile(%v, %v, %v, %v)", tt.args.ctx, tt.args.typ, tt.args.configPath, tt.args.cfg))
		})
	}
}

var _ logservice.BRHAKeeperClient = new(dumpHakeeper)

const (
	backupData = "backup_data"
)

type dumpHakeeper struct {
}

func (d *dumpHakeeper) Close() error {
	//TODO implement me
	panic("implement me")
}

func (d *dumpHakeeper) GetBackupData(ctx context.Context) ([]byte, error) {
	return []byte(backupData), nil
}

func Test_backupHakeeper(t *testing.T) {
	type args struct {
		ctx    context.Context
		config *Config
	}
	etlFs := getTestFs(t, true)
	taeFs := getTestFs(t, false)

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx:    context.Background(),
				config: nil,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return false
			},
		},
		{
			name: "t2",
			args: args{
				ctx: context.Background(),
				config: &Config{
					Timestamp:  types.TS{},
					GeneralDir: nil,
					SharedFs:   nil,
					TaeDir:     nil,
					HAkeeper:   nil,
					Metas:      &Metas{},
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return false
			},
		},
		{
			name: "t3",
			args: args{
				ctx: context.Background(),
				config: &Config{
					Timestamp:  types.TS{},
					GeneralDir: etlFs,
					SharedFs:   nil,
					TaeDir:     etlFs,
					HAkeeper:   &dumpHakeeper{},
					Metas:      &Metas{},
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				check, err2 := readFileAndCheck(context.Background(), etlFs, hakeeperDir+"/"+HakeeperFile)
				assert.NoError(t, err2)
				assert.Equal(t, check, []byte(backupData))
				return false
			},
		},
		{
			name: "t4",
			args: args{
				ctx: context.Background(),
				config: &Config{
					Timestamp:  types.TS{},
					GeneralDir: etlFs,
					SharedFs:   nil,
					TaeDir:     taeFs,
					HAkeeper:   &dumpHakeeper{},
					Metas:      &Metas{},
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.NoError(t, err)
				check, err2 := readFileAndCheck(context.Background(), taeFs, hakeeperDir+"/"+HakeeperFile)
				assert.NoError(t, err2)
				assert.Equal(t, check, []byte(backupData))
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, backupHakeeper(tt.args.ctx, tt.args.config), fmt.Sprintf("backupHakeeper(%v, %v)", tt.args.ctx, tt.args.config))
		})
	}
}

func TestBackup(t *testing.T) {
	type args struct {
		ctx context.Context
		bs  *tree.BackupStart
		cfg *Config
	}

	stubs := gostub.StubFunc(&backupTae, nil)
	defer stubs.Reset()

	tDir := getTempDir(t, "test")
	tf1 := getTempFile(t, "", "t1", "test_t1")

	bs := &tree.BackupStart{
		IsS3:        false,
		Dir:         tDir,
		Parallelism: "10",
	}

	//backup configs
	SaveLaunchConfigPath(CnConfig, []string{tf1.Name()})

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx: nil,
				bs:  bs,
				cfg: nil,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Error(t, err)
				return false
			},
		},
		{
			name: "t2",
			args: args{
				ctx: context.Background(),
				bs:  bs,
				cfg: &Config{
					Timestamp:  types.TS{},
					GeneralDir: nil,
					SharedFs:   nil,
					TaeDir:     nil,
					HAkeeper:   &dumpHakeeper{},
					Metas:      NewMetas(),
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				cfg := i[0].(*Config)
				assert.NoError(t, err)
				assert.NotNil(t, cfg)

				//checkup config files
				list, err2 := cfg.GeneralDir.List(context.Background(), configDir)
				assert.NoError(t, err2)
				var configFile string
				for _, entry := range list {
					if entry.IsDir {
						continue
					}
					configFile = entry.Name
					break
				}
				check, err2 := readFileAndCheck(context.Background(), cfg.GeneralDir, configDir+"/"+configFile)
				assert.NoError(t, err2)
				assert.Equal(t, check, []byte("test_t1"))

				//check hakeeper files
				check, err2 = readFileAndCheck(context.Background(), cfg.TaeDir, hakeeperDir+"/"+HakeeperFile)
				assert.NoError(t, err2)
				assert.Equal(t, check, []byte(backupData))

				//check metas
				check, err2 = readFileAndCheck(context.Background(), cfg.GeneralDir, moMeta)
				assert.NoError(t, err2)
				lines, err2 := fromCsvBytes(check)
				assert.NoError(t, err2)
				assert.Equal(t, lines[0][0], "version")
				assert.Equal(t, lines[0][1], Version)
				assert.Equal(t, lines[1][0], "buildinfo")
				assert.Equal(t, lines[1][1], buildInfo())
				assert.Equal(t, lines[2][0], "launchconfig")
				assert.Equal(t, lines[2][1], CnConfig)
				assert.Equal(t, lines[2][2], cfg.Metas.metas[2].LaunchConfigFile)
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtime.RunTest(
				"",
				func(rt runtime.Runtime) {
					tt.wantErr(t, Backup(tt.args.ctx, "", tt.args.bs, tt.args.cfg), tt.args.cfg, fmt.Sprintf("Backup(%v, %v, %v)", tt.args.ctx, tt.args.bs, tt.args.cfg))
				},
			)
		})
	}
}
