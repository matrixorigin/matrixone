// Copyright 2024 Matrix Origin
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

package pdf

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPdfToText(t *testing.T) {

	bytes, err := os.ReadFile("test/test3.pdf")
	require.Nil(t, err)

	_, err = exec.LookPath("pdftotext")
	if err == nil {
		data, err := GetPlainTextFromPdfToText(bytes)
		require.Nil(t, err)

		require.Equal(t, string(data), "Optimizing MySQL','In this tutorial, we show ...")
	}
}

func TestGoPdf(t *testing.T) {

	bytes, err := os.ReadFile("test/test3.pdf")
	require.Nil(t, err)

	data, err := GetPlainTextFromDslipakPdf(bytes)
	require.Nil(t, err)

	require.Equal(t, string(data), "OptimizingMySQL','Inthistutorial,weshow...")
}

func TestPdf(t *testing.T) {

	bytes, err := os.ReadFile("test/test3.pdf")
	require.Nil(t, err)

	data, err := GetPlainText(bytes)
	require.Nil(t, err)

	_, err = exec.LookPath("pdftotext")
	if err != nil {
		require.Equal(t, string(data), "OptimizingMySQL','Inthistutorial,weshow...")
	} else {
		require.Equal(t, string(data), "Optimizing MySQL','In this tutorial, we show ...")
	}
}
