package cdc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsumer(t *testing.T) {
	r := NewTxnRetriever(nil)

	consumer, err := NewIndexConsumer()
	require.NoError(t, err)
	err = consumer.Consume(r)
	require.NoError(t, err)
	consumer.Reset()
	consumer.Close()
}
