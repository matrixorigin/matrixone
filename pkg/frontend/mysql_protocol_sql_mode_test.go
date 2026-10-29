// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.

package frontend

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestMysqlProtocolSQLModeStatus(t *testing.T) {
	ses := newSes(nil, gomock.NewController(t))
	defer ses.Close()
	wire := &prepareResponseCaptureConn{}
	ioSession, err := NewIOSession(wire, getPu(""), "")
	require.NoError(t, err)
	proto := NewMysqlClientProtocol("", 42, ioSession, 1024, getPu("").SV)
	proto.SetSession(ses)
	proto.capability = CLIENT_PROTOCOL_41
	const unrelated = uint16(SERVER_STATUS_AUTOCOMMIT | SERVER_STATUS_IN_TRANS | SERVER_MORE_RESULTS_EXISTS)
	for _, mode := range []string{"NO_BACKSLASH_ESCAPES", "", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES", "STRICT_TRANS_TABLES"} {
		require.NoError(t, ses.SetSessionSysVar(context.Background(), "sql_mode", mode))
		want := unrelated
		if mode == "NO_BACKSLASH_ESCAPES" || mode == "ANSI_QUOTES,NO_BACKSLASH_ESCAPES" {
			want |= SERVER_STATUS_NO_BACKSLASH_ESCAPES
		}
		for _, packet := range [][]byte{
			proto.MakeOKPayload(0, 0, unrelated|SERVER_STATUS_NO_BACKSLASH_ESCAPES, 7, ""),
			proto.makeOKPayloadWithEof(0, 0, proto.responseStatus(unrelated|SERVER_STATUS_NO_BACKSLASH_ESCAPES), 7, ""),
			proto.MakeEOFPayload(7, unrelated|SERVER_STATUS_NO_BACKSLASH_ESCAPES),
		} {
			status := binary.LittleEndian.Uint16(packet[HeaderOffset+3 : HeaderOffset+5])
			require.Equal(t, want, status&(unrelated|SERVER_STATUS_NO_BACKSLASH_ESCAPES), mode)
		}

		result := NewMysqlExecutionResult(0, 0, 0, 0, newMrsForConnectionId([][]interface{}{{uint64(7)}}))
		for _, capability := range []uint32{CLIENT_PROTOCOL_41, CLIENT_PROTOCOL_41 | CLIENT_DEPRECATE_EOF} {
			proto.capability = capability
			for _, send := range []func() error{
				func() error {
					return proto.SendResponse(context.Background(), NewGeneralOkResponse(COM_QUERY, unrelated))
				},
				func() error {
					return proto.SendResponse(context.Background(), NewResponse(ResultResponse, 0, 0, 0, unrelated, int(COM_QUERY), result))
				},
				func() error { return proto.WriteEOFOrOK(0, unrelated) },
			} {
				wire.writes = nil
				require.NoError(t, send())
				packets := splitProtocolPackets(t, wire.writes)
				require.NotEmpty(t, packets)
				final := packets[len(packets)-1]
				status := binary.LittleEndian.Uint16(final[3:5])
				require.Equal(t, want, status&(unrelated|SERVER_STATUS_NO_BACKSLASH_ESCAPES), mode)
			}
		}
	}
	proto.SetSession(nil)
	require.Equal(t, unrelated, proto.responseStatus(unrelated|SERVER_STATUS_NO_BACKSLASH_ESCAPES))
}
