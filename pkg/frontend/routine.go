package frontend

import (
	"fmt"
	"github.com/fagongzi/goetty"
	pConfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"matrixone/pkg/config"
	"net"
)

// Routine handles requests.
// Read requests from the IOSession layer,
// use the executor to handle requests, and response them.
type Routine struct {
	//protocol layer
	protocol *MysqlProtocol

	//execution layer
	executor CmdExecutor

	//io data
	io goetty.IOSession

	//the related session
	ses *Session

	// whether the handshake succeeded
	established bool

	// current username
	user string

	// current db name
	db string

	//epoch gc handler
	pdHook *PDCallbackImpl
}

func (routine *Routine) GetClientProtocol() Protocol {
	return routine.protocol
}

func (routine *Routine) GetCmdExecutor() CmdExecutor {
	return routine.executor
}

func (routine *Routine) GetSession() *Session {
	return routine.ses
}

func (routine *Routine) GetPDCallback() pConfig.ContainerHeartbeatDataProcessor {
	return routine.pdHook
}

func (routine *Routine) getConnID() uint32 {
	return routine.protocol.ConnectionID()
}

func (routine *Routine) Quit() {
	_ = routine.io.Close()
}

// Peer gets the address [Host:Port] of the client
func (routine *Routine) Peer() (string, string) {
	addr := routine.io.RemoteAddr()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		fmt.Printf("get peer host:port failed. error:%v ", err)
		return "failed", "0"
	}
	return host, port
}

func (routine *Routine) ChangeDB(db string) (*Response, error) {
	//TODO: check meta data
	var err error = nil
	if _, err = config.StorageEngine.Database(db); err != nil {
		//echo client. no such database
		return nil, NewMysqlError(ER_BAD_DB_ERROR, db)
	}
	oldDB := routine.db
	routine.db = db

	fmt.Printf("User %s change database from [%s] to [%s]\n", routine.user, oldDB, routine.db)

	resp := NewResponse(OkResponse, 0, int(COM_INIT_DB), nil)

	return resp, nil
}

func (routine *Routine) handleHandshake(payload []byte) error {
	if len(payload) < 2 {
		return fmt.Errorf("received a broken response packet")
	}

	protocol := routine.protocol
	var authResponse []byte
	if capabilities, _, ok := protocol.io.ReadUint16(payload, 0); !ok {
		return fmt.Errorf("read capabilities from response packet failed")
	} else if uint32(capabilities)&CLIENT_PROTOCOL_41 != 0 {
		var resp41 response41
		var ok bool
		var err error
		if ok, resp41, err = protocol.analyseHandshakeResponse41(payload); !ok {
			return err
		}

		authResponse = resp41.authResponse
		protocol.capability = DefaultCapability & resp41.capabilities

		if nameAndCharset, ok := collationID2CharsetAndName[int(resp41.collationID)]; !ok {
			return fmt.Errorf("get collationName and charset failed")
		} else {
			protocol.collationID = int(resp41.collationID)
			protocol.collationName = nameAndCharset.collationName
			protocol.charset = nameAndCharset.charset
		}

		protocol.maxClientPacketSize = resp41.maxPacketSize
		protocol.username = resp41.username
		routine.user = resp41.username
		routine.db = resp41.database
	} else {
		var resp320 response320
		var ok bool
		var err error
		if ok, resp320, err = protocol.analyseHandshakeResponse320(payload); !ok {
			return err
		}

		authResponse = resp320.authResponse
		protocol.capability = DefaultCapability & resp320.capabilities
		protocol.collationID = int(Utf8mb4CollationID)
		protocol.collationName = "utf8mb4_general_ci"
		protocol.charset = "utf8mb4"

		protocol.maxClientPacketSize = resp320.maxPacketSize
		protocol.username = resp320.username
		routine.user = resp320.username
		routine.db = resp320.database
	}

	if err := protocol.authenticateUser(authResponse); err != nil {
		return err
	}

	okPkt := protocol.makeOKPayload(0, 0, 0, 0, "")
	err := routine.io.WriteAndFlush(protocol.makePackets(okPkt))
	if err != nil {
		return err
	}
	fmt.Println("SWITCH ESTABLISHED to true")
	routine.established = true
	return nil
}

func NewRoutine(rs goetty.IOSession, protocol *MysqlProtocol, executor CmdExecutor, session *Session) *Routine {
	ri := &Routine{
		protocol:    protocol,
		executor:    executor,
		ses:         session,
		io:          rs,
		established: false,
	}

	if protocol != nil {
		protocol.SetRoutine(ri)
	}

	if executor != nil {
		executor.SetRoutine(ri)
	}

	return ri
}