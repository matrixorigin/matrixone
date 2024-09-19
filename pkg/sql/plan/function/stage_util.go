// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"container/list"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"golang.org/x/sync/errgroup"

	//"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const STAGE_PROTOCOL = "stage"
const S3_PROTOCOL = "s3"
const FILE_PROTOCOL = "file"

const PARAMKEY_AWS_KEY_ID = "aws_key_id"
const PARAMKEY_AWS_SECRET_KEY = "aws_secret_key"
const PARAMKEY_AWS_REGION = "aws_region"
const PARAMKEY_ENDPOINT = "endpoint"
const PARAMKEY_COMPRESSION = "compression"
const PARAMKEY_PROVIDER = "provider"

const S3_PROVIDER_AMAZON = "amazon"
const S3_PROVIDER_MINIO = "minio"
const S3_PROVIDER_COS = "cos"

const S3_SERVICE = "s3"
const MINIO_SERVICE = "minio"

type StageDef struct {
	Id          uint32
	Name        string
	Url         *url.URL
	Credentials map[string]string
	Status      string
}

func (s *StageDef) GetCredentials(key string, defval string) (string, bool) {
	if s.Credentials == nil {
		// no credential in this stage
		return defval, false
	}

	k := strings.ToLower(key)
	res, ok := s.Credentials[k]
	if !ok {
		return defval, false
	}
	return res, ok
}

func (s *StageDef) expandSubStage(proc *process.Process) (StageDef, error) {
	if s.Url.Scheme == STAGE_PROTOCOL {
		stagename, prefix, query, err := ParseStageUrl(s.Url)
		if err != nil {
			return StageDef{}, err
		}

		res, err := StageLoadCatalog(proc, stagename)
		if err != nil {
			return StageDef{}, err
		}

		res.Url = res.Url.JoinPath(prefix)
		res.Url.RawQuery = query
		return res.expandSubStage(proc)
	}

	return *s, nil
}

// get stages and expand the path. stage may be a file or s3
// use the format of path  s3,<endpoint>,<region>,<bucket>,<key>,<secret>,<prefix>
// or minio,<endpoint>,<region>,<bucket>,<key>,<secret>,<prefix>
// expand the subpath to MO path.
// subpath is in the format like path or path with query like path?q1=v1&q2=v2...
func (s *StageDef) ToPath() (mopath string, query string, err error) {

	if s.Url.Scheme == S3_PROTOCOL {
		bucket, prefix, query, err := ParseS3Url(s.Url)
		if err != nil {
			return "", "", err
		}

		// get S3 credentials
		aws_key_id, found := s.GetCredentials(PARAMKEY_AWS_KEY_ID, "")
		if !found {
			return "", "", moerr.NewBadConfig(context.TODO(), "Stage credentials: AWS_KEY_ID not found")
		}
		aws_secret_key, found := s.GetCredentials(PARAMKEY_AWS_SECRET_KEY, "")
		if !found {
			return "", "", moerr.NewBadConfig(context.TODO(), "Stage credentials: AWS_SECRET_KEY not found")
		}
		aws_region, found := s.GetCredentials(PARAMKEY_AWS_REGION, "")
		if !found {
			return "", "", moerr.NewBadConfig(context.TODO(), "Stage credentials: AWS_REGION not found")
		}
		provider, found := s.GetCredentials(PARAMKEY_PROVIDER, "")
		if !found {
			return "", "", moerr.NewBadConfig(context.TODO(), "Stage credentials: PROVIDER not found")
		}
		endpoint, found := s.GetCredentials(PARAMKEY_ENDPOINT, "")
		if !found {
			return "", "", moerr.NewBadConfig(context.TODO(), "Stage credentials: ENDPOINT not found")
		}

		service, err := getS3ServiceFromProvider(provider)
		if err != nil {
			return "", "", err
		}

		buf := new(strings.Builder)
		w := csv.NewWriter(buf)
		opts := []string{service, endpoint, aws_region, bucket, aws_key_id, aws_secret_key, ""}

		if err = w.Write(opts); err != nil {
			return "", "", err
		}
		w.Flush()
		return fileservice.JoinPath(buf.String(), prefix), query, nil
	} else if s.Url.Scheme == FILE_PROTOCOL {
		return s.Url.Path, s.Url.RawQuery, nil
	}
	return "", "", moerr.NewBadConfigf(context.TODO(), "URL protocol %s not supported", s.Url.Scheme)
}

func getS3ServiceFromProvider(provider string) (string, error) {
	provider = strings.ToLower(provider)
	switch provider {
	case S3_PROVIDER_COS:
		return S3_SERVICE, nil
	case S3_PROVIDER_AMAZON:
		return S3_SERVICE, nil
	case S3_PROVIDER_MINIO:
		return MINIO_SERVICE, nil
	default:
		return "", moerr.NewBadConfigf(context.TODO(), "provider %s not supported", provider)
	}
}

func runSql(proc *process.Process, sql string) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}
	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithDatabase(proc.GetSessionInfo().Database).
		WithTimeZone(proc.GetSessionInfo().TimeZone).
		WithAccountID(proc.GetSessionInfo().AccountId)
	return exec.Exec(proc.GetTopContext(), sql, opts)
}

func credentialsToMap(cred string) (map[string]string, error) {
	if len(cred) == 0 {
		return nil, nil
	}

	opts := strings.Split(cred, ",")
	if len(opts) == 0 {
		return nil, nil
	}

	credentials := make(map[string]string)
	for _, o := range opts {
		kv := strings.SplitN(o, "=", 2)
		if len(kv) != 2 {
			return nil, moerr.NewBadConfig(context.TODO(), "Format error: invalid stage credentials")
		}
		credentials[strings.ToLower(kv[0])] = kv[1]
	}

	return credentials, nil
}

func StageLoadCatalog(proc *process.Process, stagename string) (s StageDef, err error) {
	getAllStagesSql := fmt.Sprintf("select stage_id, stage_name, url, stage_credentials, stage_status from `%s`.`%s` WHERE stage_name = '%s';", "mo_catalog", "mo_stages", stagename)
	res, err := runSql(proc, getAllStagesSql)
	if err != nil {
		return StageDef{}, err
	}
	defer res.Close()

	var reslist []StageDef
	const id_idx = 0
	const name_idx = 1
	const url_idx = 2
	const cred_idx = 3
	const status_idx = 4
	if res.Batches != nil {
		for _, batch := range res.Batches {
			if batch != nil && batch.Vecs[0] != nil && batch.Vecs[0].Length() > 0 {
				for i := 0; i < batch.Vecs[0].Length(); i++ {
					stage_id := vector.GetFixedAtWithTypeCheck[uint32](batch.Vecs[id_idx], i)
					stage_name := string(batch.Vecs[name_idx].GetBytesAt(i))
					stage_url, err := url.Parse(string(batch.Vecs[url_idx].GetBytesAt(i)))
					if err != nil {
						return StageDef{}, err
					}
					stage_cred := string(batch.Vecs[cred_idx].GetBytesAt(i))

					credmap, err := credentialsToMap(stage_cred)
					if err != nil {
						return StageDef{}, err
					}

					stage_status := string(batch.Vecs[status_idx].GetBytesAt(i))

					//logutil.Infof("CATALOG: ID %d,  stage %s url %s cred %s", stage_id, stage_name, stage_url, stage_cred)
					reslist = append(reslist, StageDef{stage_id, stage_name, stage_url, credmap, stage_status})
				}
			}
		}
	}

	if reslist == nil {
		return StageDef{}, moerr.NewBadConfigf(context.TODO(), "Stage %s not found", stagename)
	}

	return reslist[0], nil
}

func UrlToPath(furl string, proc *process.Process) (path string, query string, err error) {

	s, err := UrlToStageDef(furl, proc)
	if err != nil {
		return "", "", err
	}

	return s.ToPath()
}

func ParseStageUrl(u *url.URL) (stagename, prefix, query string, err error) {
	if u.Scheme != STAGE_PROTOCOL {
		return "", "", "", moerr.NewBadConfig(context.TODO(), "ParseStageUrl: URL protocol is not stage://")
	}

	stagename = u.Host
	if len(stagename) == 0 {
		return "", "", "", moerr.NewBadConfig(context.TODO(), "Invalid stage URL: stage name is empty string")
	}

	prefix = u.Path
	query = u.RawQuery

	return
}

func ParseS3Url(u *url.URL) (bucket, fpath, query string, err error) {
	bucket = u.Host
	fpath = u.Path
	query = u.RawQuery
	err = nil

	if len(bucket) == 0 {
		err = moerr.NewBadConfig(context.TODO(), "Invalid s3 URL: bucket is empty string")
		return "", "", "", err
	}

	return
}

func UrlToStageDef(furl string, proc *process.Process) (s StageDef, err error) {

	aurl, err := url.Parse(furl)
	if err != nil {
		return StageDef{}, err
	}

	if aurl.Scheme != STAGE_PROTOCOL {
		return StageDef{}, moerr.NewBadConfig(context.TODO(), "URL is not stage URL")
	}

	stagename, subpath, query, err := ParseStageUrl(aurl)
	if err != nil {
		return StageDef{}, err
	}

	sdef, err := StageLoadCatalog(proc, stagename)
	if err != nil {
		return StageDef{}, err
	}

	s, err = sdef.expandSubStage(proc)
	if err != nil {
		return StageDef{}, err
	}

	s.Url = s.Url.JoinPath(subpath)
	s.Url.RawQuery = query

	return s, nil
}

func stageListWithWildcard(service string, pattern string, proc *process.Process) (fileList []string, err error) {
	const wildcards = "*?"
	const sep = "/"
	fs := proc.GetFileService()

	idx := strings.IndexAny(pattern, wildcards)
	if idx == -1 {
		return nil, moerr.NewInternalError(proc.Ctx, "pattern without wildcard")
	}

	var pathDir []string
	idx = strings.LastIndex(pattern[:idx], sep)
	if idx == -1 {
		pathDir = append(pathDir, "")
		pathDir = append(pathDir, strings.Split(pattern, sep)...)
	} else {
		pathDir = append(pathDir, pattern[:idx])
		pathDir = append(pathDir, strings.Split(pattern[idx+1:], sep)...)
	}

	l := list.New()
	l2 := list.New()
	if pathDir[0] == "" {
		l.PushBack(sep)
	} else {
		l.PushBack(pathDir[0])
	}

	for i := 1; i < len(pathDir); i++ {
		length := l.Len()
		for j := 0; j < length; j++ {
			prefix := l.Front().Value.(string)
			p := fileservice.JoinPath(service, prefix)
			etlfs, readpath, err := fileservice.GetForETL(proc.Ctx, fs, p)
			if err != nil {
				return nil, err
			}
			entries, err := etlfs.List(proc.Ctx, readpath)
			if err != nil {
				return nil, err
			}
			for _, entry := range entries {
				if !entry.IsDir && i+1 != len(pathDir) {
					continue
				}
				if entry.IsDir && i+1 == len(pathDir) {
					continue
				}
				matched, err := path.Match(pathDir[i], entry.Name)
				if err != nil {
					return nil, err
				}
				if !matched {
					continue
				}
				l.PushBack(path.Join(l.Front().Value.(string), entry.Name))
				if !entry.IsDir {
					l2.PushBack(entry.Size)
				}
			}
			l.Remove(l.Front())
		}
	}
	length := l.Len()

	for j := 0; j < length; j++ {
		fileList = append(fileList, l.Front().Value.(string))
		l.Remove(l.Front())
		//fileSize = append(fileSize, l2.Front().Value.(int64))
		l2.Remove(l2.Front())
	}

	return fileList, nil
}

func stageListWithoutWildcard(service string, pattern string, proc *process.Process) (fileList []string, err error) {

	fs := proc.GetFileService()
	p := fileservice.JoinPath(service, pattern)
	etlfs, readpath, err := fileservice.GetForETL(proc.Ctx, fs, p)
	if err != nil {
		return nil, err
	}
	entries, err := etlfs.List(proc.Ctx, readpath)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		fileList = append(fileList, path.Join(pattern, entry.Name))
	}

	return fileList, nil
}

func StageListWithPattern(service string, pattern string, proc *process.Process) (fileList []string, err error) {
	const wildcards = "*?"

	idx := strings.IndexAny(pattern, wildcards)
	if idx == -1 {
		// no wildcard in pattern
		fileList, err = stageListWithoutWildcard(service, pattern, proc)
		if err != nil {
			return nil, err
		}
	} else {
		fileList, err = stageListWithWildcard(service, pattern, proc)
		if err != nil {
			return nil, err
		}
	}
	return fileList, nil
}

// ParseDatalink extracts data from a Datalink string
// and returns the Mo FS url, []int{offset,size}, fileType and error
// Mo FS url: The URL that is used by MO FS to access the file
// offsetSize: The offset and size of the file to be read
func ParseDatalink(fsPath string, proc *process.Process) (string, []int, error) {
	u, err := url.Parse(fsPath)
	if err != nil {
		return "", nil, err
	}

	var moUrl string
	// 1. get moUrl from the path
	switch u.Scheme {
	case FILE_PROTOCOL:
		moUrl = strings.Join([]string{u.Host, u.Path}, "")
	case STAGE_PROTOCOL:
		moUrl, _, err = UrlToPath(fsPath, proc)
		if err != nil {
			return "", nil, err
		}
	default:
		return "", nil, moerr.NewNYINoCtxf("unsupported url scheme %s", u.Scheme)
	}

	// 2. get size and offset from the query
	urlParams := make(map[string]string)
	for k, v := range u.Query() {
		urlParams[strings.ToLower(k)] = strings.ToLower(v[0])
	}
	offsetSize := []int{0, -1}
	if _, ok := urlParams["offset"]; ok {
		if offsetSize[0], err = strconv.Atoi(urlParams["offset"]); err != nil {
			return "", nil, err
		}
	}
	if _, ok := urlParams["size"]; ok {
		if offsetSize[1], err = strconv.Atoi(urlParams["size"]); err != nil {
			return "", nil, err
		}
	}

	if offsetSize[0] < 0 {
		return "", nil, moerr.NewInternalErrorNoCtx("offset cannot be negative")
	}

	if offsetSize[1] < -1 {
		return "", nil, moerr.NewInternalErrorNoCtx("size cannot be less than -1")
	}

	return moUrl, offsetSize, nil
}

type FileServiceWriter struct {
	Reader      *io.PipeReader
	Writer      *io.PipeWriter
	Group       *errgroup.Group
	Filepath    string
	FileService fileservice.FileService
}

func NewFileServiceWriter(moPath string, proc *process.Process) (*FileServiceWriter, error) {

	var readPath string
	var err error

	w := &FileServiceWriter{}

	w.Filepath = moPath

	w.Reader, w.Writer = io.Pipe()

	w.FileService, readPath, err = fileservice.GetForETL(proc.Ctx, nil, w.Filepath)
	if err != nil {
		return nil, err
	}

	asyncWriteFunc := func() error {
		vec := fileservice.IOVector{
			FilePath: readPath,
			Entries: []fileservice.IOEntry{
				{
					ReaderForWrite: w.Reader,
					Size:           -1,
				},
			},
		}
		err := w.FileService.Write(proc.Ctx, vec)
		if err != nil {
			err2 := w.Reader.CloseWithError(err)
			if err2 != nil {
				return err2
			}
		}
		return err
	}

	w.Group, _ = errgroup.WithContext(proc.Ctx)
	w.Group.Go(asyncWriteFunc)

	return w, nil
}

func (w *FileServiceWriter) Write(b []byte) (int, error) {
	n, err := w.Writer.Write(b)
	if err != nil {
		err2 := w.Writer.CloseWithError(err)
		if err2 != nil {
			return 0, err2
		}
	}
	return n, err
}

func (w *FileServiceWriter) Close() error {
	err := w.Writer.Close()
	err2 := w.Group.Wait()
	err3 := w.Reader.Close()
	err = errors.Join(err, err2, err3)

	w.Reader = nil
	w.Writer = nil
	w.Group = nil
	return err
}
