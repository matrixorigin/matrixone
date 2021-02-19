package metadata

import "encoding/gob"

func init() {
	gob.Register(Attribute{})
}
