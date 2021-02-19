package logEngine

import (
	"matrixbase/pkg/vm/engine/logEngine/meta"
	"matrixbase/pkg/vm/engine/logEngine/meta/client"
)

type LogEngine struct {
	cli client.Client
}

type Relation struct {
	id string
	md meta.Metadata
}
