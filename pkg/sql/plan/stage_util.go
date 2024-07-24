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

package plan

import (
	//"context"
	"encoding/csv"
	"fmt"
	//"path"
	"net/url"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	//moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const STAGE_PROTOCOL = "stage"
const S3_PROTOCOL = "s3"
const FILE_PROTOCOL = "file"

type StageKey struct {
	Db   string
	Name string
}

type StageDef struct {
	Id          uint32
	Name        string
	Db          string
	Url         *url.URL
	Credentials string
	Disabled    bool
}

func (s *StageDef) expandSubStage(stagemap map[StageKey]StageDef, defaultdb string) (StageDef, error) {
	if s.Url.Scheme == STAGE_PROTOCOL {
		dbname, stagename, prefix, query, err := parseStageUrl(s.Url)
		if err != nil {
			return StageDef{}, err
		}

		if len(dbname) == 0 {
			dbname = defaultdb
		}

		key := StageKey{dbname, stagename}
		res, ok := stagemap[key]
		if !ok {
			return StageDef{}, fmt.Errorf("stage not found. stage://%s/%s", dbname, stagename)
		}

		res.Url = res.Url.JoinPath(prefix)
		res.Url.RawQuery = query
		return res.expandSubStage(stagemap, defaultdb)
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
		bucket, prefix, query := parseS3Url(s.Url)

		// TODO: Decode credentials
		aws_key_id := "aws_key_id"
		aws_secret_key := "aws_secret_key"
		aws_region := "aws_region"
		provider := "amazon"
		endpoint := "endpoint"

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
		logutil.Infof("ToPath: prefix = %s, query = %s", s.Url.Path, s.Url.RawQuery)
		return s.Url.Path, s.Url.RawQuery, nil
	}
	return "", "", nil
}

func getS3ServiceFromProvider(provider string) (string, error) {
	provider = strings.ToLower(provider)
	switch provider {
	case "amazon":
		return "s3", nil
	case "minio":
		return "minio", nil
	default:
		return "", fmt.Errorf("provider %s not supported", provider)
	}
}

func StageLoadCatalog(proc *process.Process) (stagemap map[StageKey]StageDef, err error) {
	getAllStagesSql := fmt.Sprintf("select stage_id, stage_name, url, stage_credentials, stage_status, 'dbname' from `%s`.`%s`;", "mo_catalog", "mo_stages")
	res, err := proc.GetSessionInfo().SqlHelper.ExecSql(getAllStagesSql)

	if err != nil {
		return nil, err
	}

	stagemap = make(map[StageKey]StageDef)
	const id_idx = 0
	const name_idx = 1
	const url_idx = 2
	const cred_idx = 3
	const status_idx = 4
	const db_idx = 5
	if res != nil {
		for _, row := range res {
			for i := 0; i < len(row); i++ {
				stage_id := row[id_idx].(uint32)
				stage_name := row[name_idx].(string)
				stage_url, err := url.Parse(row[url_idx].(string))
				if err != nil {
					return nil, err
				}
				stage_cred := row[cred_idx].(string)
				stage_status := row[status_idx].(string)
				dbname := row[db_idx].(string)
				disabled := false
				if stage_status == "disabled" {
					disabled = true
				}

				key := StageKey{dbname, stage_name}
				stagemap[key] = StageDef{stage_id, stage_name, dbname, stage_url, stage_cred, disabled}
				logutil.Infof("CATALOG: ID %d,  stage %s url %s cred %s", stage_id, stage_name, stage_url, stage_cred)
			}

		}
	}

	return stagemap, nil
}

func UrlToPath(furl string, stagemap map[StageKey]StageDef, proc *process.Process) (path string, query string, err error) {

	s, err := urlToStageDef(furl, stagemap, proc)
	if err != nil {
		return "", "", err
	}

	return s.ToPath()
}

func parseStageUrl(u *url.URL) (dbname, stagename, prefix, query string, err error) {
	if u.Scheme != STAGE_PROTOCOL {
		return "", "", "", "", fmt.Errorf("URL protocol is not stage://")
	}

	dbname = u.Host

	if len(u.Path) == 0 {
		return "", "", "", "", fmt.Errorf("Invalid stage URL: path is empty string")
	}

	pp := strings.SplitN(u.Path[1:], "/", 2)
	if len(pp) == 0 {
		return "", "", "", "", fmt.Errorf("Invalid stage URL: path not found")
	}

	stagename = pp[0]
	prefix = ""
	if len(pp) == 2 {
		prefix = pp[1]
	}

	if len(stagename) == 0 {
		return "", "", "", "", fmt.Errorf("Invalid stage URL: stage name not found")
	}

	query = u.RawQuery

	return
}

func parseS3Url(u *url.URL) (bucket, fpath, query string) {
	bucket = u.Host
	fpath = u.Path
	query = u.RawQuery
	return
}

func urlToStageDef(furl string, stagemap map[StageKey]StageDef, proc *process.Process) (s StageDef, err error) {

	aurl, err := url.Parse(furl)
	if err != nil {
		return StageDef{}, err
	}

	if aurl.Scheme != STAGE_PROTOCOL {
		return StageDef{}, fmt.Errorf("URL is not stage URL")
	}

	dbname, stagename, subpath, query, err := parseStageUrl(aurl)
	if err != nil {
		return StageDef{}, fmt.Errorf("Parse Error: Invalid stage URL")
	}

	curdb := proc.GetSessionInfo().Database
	logutil.Infof("Current database = %s, URL = %s", curdb, aurl)

	if len(dbname) == 0 {
		dbname = curdb
	}
	logutil.Infof("UrlToPath dbname %s, stagename %s, subpath %s", dbname, stagename, subpath)
	key := StageKey{dbname, stagename}
	s, ok := stagemap[key]
	if !ok {
		return StageDef{}, fmt.Errorf("stage %s not found", stagename)
	}

	exs, err := s.expandSubStage(stagemap, curdb)
	if err != nil {
		return StageDef{}, err
	}

	logutil.Infof("ExanpdSubStage Url=%s", exs.Url)

	exs.Url = exs.Url.JoinPath(subpath)
	exs.Url.RawQuery = query

	logutil.Infof("JoinPath Url=%s", exs.Url)

	return exs, nil
}

func GetFilePathFromParam(param *tree.ExternParam) string {
	fpath := param.Filepath
	for i := 0; i < len(param.Option); i += 2 {
		name := strings.ToLower(param.Option[i])
		if name == "filepath" {
			fpath = param.Option[i+1]
			break
		}
	}

	return fpath
}

func InitStageS3Param(param *tree.ExternParam, s StageDef) error {

	param.ScanType = tree.S3
	param.S3Param = &tree.S3Parameter{}

	if len(s.Url.RawQuery) > 0 {
		return fmt.Errorf("s3:// Query don't support in ExternParam.S3Param")
	}

	if s.Url.Scheme != S3_PROTOCOL {
		return fmt.Errorf("protocol is not S3")
	}

	bucket, prefix, _ := parseS3Url(s.Url)

	param.S3Param.Endpoint = "endpoint"
	param.S3Param.Region = "region"
	param.S3Param.APIKey = "aws_key_id"
	param.S3Param.APISecret = "aws_secret_key"
	param.S3Param.Bucket = bucket
	param.S3Param.Provider = "minio"

	param.Filepath = prefix
	param.CompressType = "compression"

	for i := 0; i < len(param.Option); i += 2 {
		switch strings.ToLower(param.Option[i]) {
		case "format":
			format := strings.ToLower(param.Option[i+1])
			if format != tree.CSV && format != tree.JSONLINE {
				return moerr.NewBadConfig(param.Ctx, "the format '%s' is not supported", format)
			}
			param.Format = format
		case "jsondata":
			jsondata := strings.ToLower(param.Option[i+1])
			if jsondata != tree.OBJECT && jsondata != tree.ARRAY {
				return moerr.NewBadConfig(param.Ctx, "the jsondata '%s' is not supported", jsondata)
			}
			param.JsonData = jsondata
			param.Format = tree.JSONLINE

		default:
			return moerr.NewBadConfig(param.Ctx, "the keyword '%s' is not support", strings.ToLower(param.Option[i]))
		}
	}

	if param.Format == tree.JSONLINE && len(param.JsonData) == 0 {
		return moerr.NewBadConfig(param.Ctx, "the jsondata must be specified")
	}
	if len(param.Format) == 0 {
		param.Format = tree.CSV
	}

	return nil

}

func InitInfileOrStageParam(param *tree.ExternParam, proc *process.Process) error {

	fpath := GetFilePathFromParam(param)

	if !strings.HasPrefix(fpath, STAGE_PROTOCOL+"://") {
		return InitInfileParam(param)
	}

	stagemap, err := StageLoadCatalog(proc)
	if err != nil {
		return err
	}

	s, err := urlToStageDef(fpath, stagemap, proc)
	if err != nil {
		return err
	}

	if len(s.Url.RawQuery) > 0 {
		return fmt.Errorf("Invalid URL: query not supported in ExternParam")
	}

	if s.Url.Scheme == S3_PROTOCOL {
		return InitStageS3Param(param, s)
	} else if s.Url.Scheme == FILE_PROTOCOL {

		err := InitInfileParam(param)
		if err != nil {
			return err
		}

		param.Filepath = s.Url.Path

	} else {
		return fmt.Errorf("invalid URL: protocol %s not supported", s.Url.Scheme)
	}

	return nil
}
