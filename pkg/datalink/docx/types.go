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

// docx parser is to get parse the docx file and get all text content
// docx file is zip file that contains xml files. word/document.xml file inside zip is location of content located.
// Parse word/document.xml and traverse all paragraph tags <p> to get all text content. Each paragraph tag may have multiple rows <r>
// and each row <r> has a text <t>.
//
// sample xml with paragraph tag
//         <w:p w14:paraId="01895B7A" w14:textId="77777777" w:rsidR="00F553D1" w:rsidRDefault="00F553D1">
//            <w:r>
//                <w:t>This is a paragraph of text 1.</w:t>
//            </w:r>
//            <w:r>
//                <w:t>This is a paragraph of text 2.</w:t>
//            </w:r>
//        </w:p>

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
