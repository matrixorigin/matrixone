// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package docx

type WordDocument struct {
	Paragraphs []Paragraph
}

type Paragraph struct {
	Style Style `xml:"pPr>pStyle"`
	Rows  []Row `xml:"r"`
}

type Style struct {
	Val string `xml:"val,attr"`
}
type Row struct {
	Text string `xml:"t"`
}

// methods
func (w WordDocument) AsText() string {
	text := ""
	for _, v := range w.Paragraphs {
		for _, rv := range v.Rows {
			text += rv.Text
		}
		text += "\n"
	}
	return text
}
