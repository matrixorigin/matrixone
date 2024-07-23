package embed

import (
	"sync"
)

var (
	basicOnce    sync.Once
	basicCluster Cluster
)

// RunBaseClusterTests starting an integration test for a 1 log, 1tn, 3cn base cluster is very slow
// due to the amount of time it takes to start a cluster (10-20s) when there are a very large number
// of test cases. So for some special cases that don't need to be restarted, a basicCluster can be
// reused to run the test cases. in summary, the basic cluster will only be started once!
func RunBaseClusterTests(
	fn func(Cluster),
) error {
	var err error
	var c Cluster
	basicOnce.Do(
		func() {
			c, err = NewCluster(WithCNCount(3))
			if err != nil {
				return
			}
			err = c.Start()
			if err != nil {
				return
			}
			basicCluster = c
		},
	)
	if err != nil {
		return err
	}
	fn(basicCluster)
	return nil
}
