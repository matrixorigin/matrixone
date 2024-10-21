package pdf

import (
	"bytes"
	"strings"

	gopdf "github.com/dslipak/pdf"
)

func GetContent(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	pdfr, err := gopdf.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}

	npage := pdfr.NumPage()
	for i := 1; i <= npage; i++ {

		p := pdfr.Page(i)
		texts := p.Content().Text
		var lastY = 0.0
		line := ""

		for _, text := range texts {
			if lastY != text.Y {
				if lastY > 0 {
					buf.WriteString(line + "\n")
					line = text.S
				} else {
					line += text.S
				}
			} else {
				line += text.S
			}

			lastY = text.Y
		}
		buf.WriteString(line)
	}

	return []byte(strings.TrimSpace(buf.String())), nil
}
