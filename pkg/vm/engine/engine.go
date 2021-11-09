package engine

import "encoding/gob"

func init() {
	gob.Register(Attribute{})
}
