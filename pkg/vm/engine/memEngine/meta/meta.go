package meta

import "encoding/gob"

func init() {
	gob.Register(Metadata{})
}
