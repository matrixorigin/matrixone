package logEngine

import (
	"matrixbase/pkg/vm/engine"
	"matrixbase/pkg/vm/engine/logEngine/meta/client"
	"matrixbase/pkg/vm/metadata"
)

func New(cli client.Client) *LogEngine {
	return &LogEngine{cli}
}

func (e *LogEngine) Delete(name string) error {
	return nil
}

func (e *LogEngine) Create(name string, attrs []metadata.Attribute) error {
	return nil
}

func (e *LogEngine) Relations() []engine.Relation {
	return nil
}

func (e *LogEngine) Relation(name string) (engine.Relation, error) {
	return nil, nil
}
