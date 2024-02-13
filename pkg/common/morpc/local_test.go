package morpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLocalHandle(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000)
		defer cancel()

		rs.RegisterRequestHandler(func(_ context.Context, request RPCMessage, sequence uint64, cs ClientSession) error {
			return cs.Write(ctx, request.Message)
		})

		req := newTestMessage(1)
		f, err := c.Send(ctx, testAddr, req)
		assert.NoError(t, err)

		defer f.Close()
		resp, err := f.Get()
		assert.NoError(t, err)
		assert.Equal(t, req, resp)
	}, WithServerRegisterLocal(testAddr))
}
