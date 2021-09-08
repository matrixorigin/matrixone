//go:build !amd64 || appengine || !gc || noasm
// +build !amd64 appengine !gc noasm

/*
 * MinIO Cloud Storage, (C) 2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package simdcsv

// SupportedCPU will return whether the CPU is supported.
func SupportedCPU() bool {
	return false
}

func stage1PreprocessBufferEx(buf []byte, separatorChar, quoted uint64, masks *[]uint64, postProc *[]uint64) ([]uint64, []uint64, uint64) {
	return nil, nil, 0
}

func stage2ParseBufferExStreaming(buf []byte, masks []uint64, delimiterChar uint64, inputStage2 *inputStage2, outputStage2 *outputAsm, rows *[]uint64, columns *[]string) ([]uint64, []string, bool) {
	return nil, nil, false
}
