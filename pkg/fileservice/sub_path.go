// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"context"
	"fmt"
	"iter"
	"os"
	"path"
	"strings"
)

type subPathFS struct {
	upstream FileService
	path     string
	name     string
}

// SubPath returns a FileService instance that operates at specified sub path of the upstream instance
func SubPath(upstream FileService, path string) FileService {
	return &subPathFS{
		upstream: upstream,
		path:     path,
		name: strings.Join([]string{
			"sub",
			upstream.Name(),
			path,
		}, ","),
	}
}

var _ FileService = new(subPathFS)

func (s *subPathFS) Name() string {
	return s.name
}

func (s *subPathFS) toUpstreamPath(p string) (string, error) {
	parsed, err := ParsePathAtService(p, s.name)
	if err != nil {
		return "", err
	}
	parsed.File = path.Join(s.path, parsed.File)
	parsed.Service = s.upstream.Name()
	parsed.ServiceArguments = nil
	return parsed.String(), nil
}

func (s *subPathFS) Close(ctx context.Context) {
}

func (s *subPathFS) Write(ctx context.Context, vector IOVector) error {
	p, err := s.toUpstreamPath(vector.FilePath)
	if err != nil {
		return err
	}
	vector.FilePath = p
	return s.upstream.Write(ctx, vector)
}

func (s *subPathFS) Read(ctx context.Context, vector *IOVector) error {
	subVector := *vector
	p, err := s.toUpstreamPath(subVector.FilePath)
	if err != nil {
		return err
	}
	subVector.FilePath = p
	return s.upstream.Read(ctx, &subVector)
}

func (s *subPathFS) ReadCache(ctx context.Context, vector *IOVector) error {
	subVector := *vector
	p, err := s.toUpstreamPath(subVector.FilePath)
	if err != nil {
		return err
	}
	subVector.FilePath = p
	return s.upstream.ReadCache(ctx, &subVector)
}

func (s *subPathFS) List(ctx context.Context, dirPath string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		p, err := s.toUpstreamPath(dirPath)
		if err != nil {
			yield(nil, err)
			return
		}
		s.upstream.List(ctx, p)(yield)
	}
}

func (s *subPathFS) Delete(ctx context.Context, filePaths ...string) error {
	if len(filePaths) == 0 {
		return nil
	}
	if len(filePaths) == 1 {
		p, err := s.toUpstreamPath(filePaths[0])
		if err != nil {
			return err
		}
		return s.upstream.Delete(ctx, p)
	}
	subPaths := make([]string, 0, len(filePaths))
	for _, p := range filePaths {
		pp, err := s.toUpstreamPath(p)
		if err != nil {
			return err
		}
		subPaths = append(subPaths, pp)
	}
	return s.upstream.Delete(ctx, subPaths...)
}

func (s *subPathFS) StatFile(ctx context.Context, filePath string) (*DirEntry, error) {
	p, err := s.toUpstreamPath(filePath)
	if err != nil {
		return nil, err
	}
	return s.upstream.StatFile(ctx, p)
}

func (s *subPathFS) PrefetchFile(ctx context.Context, filePath string) error {
	p, err := s.toUpstreamPath(filePath)
	if err != nil {
		return err
	}
	return s.upstream.PrefetchFile(ctx, p)
}

func (s *subPathFS) Cost() *CostAttr {
	return s.upstream.Cost()
}

var _ MutableFileService = new(subPathFS)

func (s *subPathFS) EnsureDir(ctx context.Context, filePath string) error {
	p, err := s.toUpstreamPath(filePath)
	if err != nil {
		return err
	}
	fs, ok := s.upstream.(MutableFileService)
	if !ok {
		panic(fmt.Sprintf("%T does not implement MutableFileService", s.upstream))
	}
	return fs.EnsureDir(ctx, p)
}

func (s *subPathFS) NewMutator(ctx context.Context, filePath string) (Mutator, error) {
	p, err := s.toUpstreamPath(filePath)
	if err != nil {
		return nil, err
	}
	fs, ok := s.upstream.(MutableFileService)
	if !ok {
		panic(fmt.Sprintf("%T does not implement MutableFileService", s.upstream))
	}
	return fs.NewMutator(ctx, p)
}

func (s *subPathFS) OpenFile(ctx context.Context, filePath string) (*os.File, error) {
	p, err := s.toUpstreamPath(filePath)
	if err != nil {
		return nil, err
	}

	fs, ok := s.upstream.(MutableFileService)
	if !ok {
		panic(fmt.Sprintf("%T does not implement MutableFileService", s.upstream))
	}

	return fs.OpenFile(ctx, p)
}

func (s *subPathFS) CreateFile(ctx context.Context, filePath string) (*os.File, error) {
	p, err := s.toUpstreamPath(filePath)
	if err != nil {
		return nil, err
	}

	fs, ok := s.upstream.(MutableFileService)
	if !ok {
		panic(fmt.Sprintf("%T does not implement MutableFileService", s.upstream))
	}

	return fs.CreateFile(ctx, p)
}

func (s *subPathFS) RemoveFile(ctx context.Context, filePath string) error {
	p, err := s.toUpstreamPath(filePath)
	if err != nil {
		return err
	}

	fs, ok := s.upstream.(MutableFileService)
	if !ok {
		panic(fmt.Sprintf("%T does not implement MutableFileService", s.upstream))
	}

	return fs.RemoveFile(ctx, p)
}

func (s *subPathFS) CreateAndRemoveFile(ctx context.Context, filePath string) (*os.File, error) {
	p, err := s.toUpstreamPath(filePath)
	if err != nil {
		return nil, err
	}

	fs, ok := s.upstream.(MutableFileService)
	if !ok {
		panic(fmt.Sprintf("%T does not implement MutableFileService", s.upstream))
	}

	return fs.CreateAndRemoveFile(ctx, p)
}
