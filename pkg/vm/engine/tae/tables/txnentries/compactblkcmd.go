package txnentries

import "io"

type compactBlockCmd struct{}

func (cmd *compactBlockCmd) GetType() int16                   { return CmdCompactBlock }
func (cmd *compactBlockCmd) WriteTo(w io.Writer) (err error)  { return }
func (cmd *compactBlockCmd) ReadFrom(r io.Reader) (err error) { return }
func (cmd *compactBlockCmd) Marshal() (buf []byte, err error) { return }
func (cmd *compactBlockCmd) Unmarshal(buf []byte) (err error) { return }
func (cmd *compactBlockCmd) String() string                   { return "" }
