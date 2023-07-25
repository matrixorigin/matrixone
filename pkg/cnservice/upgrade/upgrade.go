package upgrade

import (
	"context"

	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

func Upgrade(ctx context.Context, ieFactory func() ie.InternalExecutor) {
	err := motrace.UpgradeSchemaByInnerExecutor(ctx, ieFactory)
	if err != nil {
		return
	}
}
