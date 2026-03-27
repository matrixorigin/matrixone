//go:build gpu

/* 
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cuvs

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// Pack consolidates multiple files into a single .tar or .tar.gz file.
// If outputPath ends with .gz, gzip compression is used.
func Pack(dirPath string, manifestJson string, outputPath string) error {
	// 1. Write manifest.json into the directory
	manifestPath := filepath.Join(dirPath, "manifest.json")
	if err := os.WriteFile(manifestPath, []byte(manifestJson), 0644); err != nil {
		return err
	}

	// 2. Create the output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	var tw *tar.Writer
	var gw *gzip.Writer

	if strings.HasSuffix(outputPath, ".gz") {
		gw = gzip.NewWriter(outFile)
		defer gw.Close()
		tw = tar.NewWriter(gw)
	} else {
		tw = tar.NewWriter(outFile)
	}
	defer tw.Close()

	// 3. Iterate over files in the directory and add them to the tar
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filePath := filepath.Join(dirPath, file.Name())
		fi, err := os.Stat(filePath)
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(fi, "")
		if err != nil {
			return err
		}
		header.Name = file.Name()

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		f, err := os.Open(filePath)
		if err != nil {
			return err
		}
		if _, err := io.Copy(tw, f); err != nil {
			f.Close()
			return err
		}
		f.Close()
	}

	return nil
}

// Unpack extracts components from a .tar or .tar.gz file into a directory and returns the manifest.
func Unpack(inputPath string, dirPath string) (string, error) {
	in, err := os.Open(inputPath)
	if err != nil {
		return "", err
	}
	defer in.Close()

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return "", err
	}

	var tr *tar.Reader
	if strings.HasSuffix(inputPath, ".gz") {
		gr, err := gzip.NewReader(in)
		if err != nil {
			return "", err
		}
		defer gr.Close()
		tr = tar.NewReader(gr)
	} else {
		tr = tar.NewReader(in)
	}

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		target := filepath.Join(dirPath, header.Name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return "", err
			}
		case tar.TypeReg:
			f, err := os.Create(target)
			if err != nil {
				return "", err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return "", err
			}
			f.Close()
		}
	}

	// Read manifest.json
	manifestPath := filepath.Join(dirPath, "manifest.json")
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return "", moerr.NewInternalErrorNoCtx(fmt.Sprintf("failed to read manifest from tar: %v", err))
	}

	return string(manifestBytes), nil
}
