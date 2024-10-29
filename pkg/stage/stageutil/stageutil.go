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

package stageutil

import (
	"container/list"
	"context"
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/stage"

	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func ExpandSubStage(s stage.StageDef, proc *process.Process) (stage.StageDef, error) {
	if s.Url.Scheme == stage.STAGE_PROTOCOL {
		stagename, prefix, query, err := stage.ParseStageUrl(s.Url)
		if err != nil {
			return stage.StageDef{}, err
		}

		res, err := StageLoadCatalog(proc, stagename)
		if err != nil {
			return stage.StageDef{}, err
		}

		res.Url = res.Url.JoinPath(prefix)
		res.Url.RawQuery = query
		return ExpandSubStage(res, proc)
	}

	return s, nil
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

func StageLoadCatalog(proc *process.Process, stagename string) (s stage.StageDef, err error) {

	cache := proc.GetStageCache()
	s, ok := cache.Get(stagename)
	if ok {
		return s, nil
	}

	getAllStagesSql := fmt.Sprintf("select stage_id, stage_name, url, stage_credentials, stage_status from `%s`.`%s` WHERE stage_name = '%s';", "mo_catalog", "mo_stages", stagename)
	res, err := runSql(proc, getAllStagesSql)
	if err != nil {
		return stage.StageDef{}, err
	}
	defer res.Close()

	var reslist []stage.StageDef
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
						return stage.StageDef{}, err
					}
					stage_cred := string(batch.Vecs[cred_idx].GetBytesAt(i))

					credmap, err := stage.CredentialsToMap(stage_cred)
					if err != nil {
						return stage.StageDef{}, err
					}

					stage_status := string(batch.Vecs[status_idx].GetBytesAt(i))

					//logutil.Infof("CATALOG: ID %d,  stage %s url %s cred %s", stage_id, stage_name, stage_url, stage_cred)
					reslist = append(reslist, stage.StageDef{Id: stage_id, Name: stage_name, Url: stage_url, Credentials: credmap, Status: stage_status})
				}
			}
		}
	}

	if reslist == nil {
		return stage.StageDef{}, moerr.NewBadConfigf(context.TODO(), "Stage %s not found", stagename)
	}

	cache.Set(stagename, reslist[0])
	return reslist[0], nil
}

func UrlToPath(furl string, proc *process.Process) (path string, query string, err error) {

	s, err := UrlToStageDef(furl, proc)
	if err != nil {
		return "", "", err
	}

	return s.ToPath()
}

func UrlToStageDef(furl string, proc *process.Process) (s stage.StageDef, err error) {

	aurl, err := url.Parse(furl)
	if err != nil {
		return stage.StageDef{}, err
	}

	if aurl.Scheme != stage.STAGE_PROTOCOL {
		return stage.StageDef{}, moerr.NewBadConfig(context.TODO(), "URL is not stage URL")
	}

	stagename, subpath, query, err := stage.ParseStageUrl(aurl)
	if err != nil {
		return stage.StageDef{}, err
	}

	sdef, err := StageLoadCatalog(proc, stagename)
	if err != nil {
		return stage.StageDef{}, err
	}

	s, err = ExpandSubStage(sdef, proc)
	if err != nil {
		return stage.StageDef{}, err
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
			for entry, err := range etlfs.List(proc.Ctx, readpath) {
				if err != nil {
					return nil, err
				}
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
	for entry, err := range etlfs.List(proc.Ctx, readpath) {
		if err != nil {
			return nil, err
		}
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
