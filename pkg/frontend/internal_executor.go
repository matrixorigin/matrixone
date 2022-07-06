// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/config"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
)

func applyOverride(sess *Session, opts ie.SessionOverrideOptions) {
	if opts.Database != nil {
		sess.protocol.SetDatabaseName(*opts.Database)
	}

	if opts.Username != nil {
		sess.protocol.SetUserName(*opts.Username)
	}

	if opts.IsInternal != nil {
		sess.IsInternal = *opts.IsInternal
	}
}

type internalExecutor struct {
	proto MysqlProtocol
	// MySqlCmdExecutor struct is used here, because we want to doComQuery directly
	executor     *MysqlCmdExecutor
	pu           *config.ParameterUnit
	baseSessOpts ie.SessionOverrideOptions
}

func NewIternalExecutor(pu *config.ParameterUnit) *internalExecutor {
	proto := &internalProtocol{}
	exec := NewMysqlCmdExecutor()

	return &internalExecutor{
		proto:        proto,
		executor:     exec,
		pu:           pu,
		baseSessOpts: ie.NewOptsBuilder().Finish(),
	}
}

func (ie *internalExecutor) Exec(sql string, opts ie.SessionOverrideOptions) error {
	sess := ie.newCmdSession(opts)
	ie.executor.PrepareSessionBeforeExecRequest(sess)
	if err := ie.executor.doComQuery(sql); err != nil {
		return err
	}

	return nil
}

func (ie *internalExecutor) newCmdSession(opts ie.SessionOverrideOptions) *Session {
	sess := NewSession(ie.proto, guest.New(ie.pu.SV.GetGuestMmuLimitation(), ie.pu.HostMmu), ie.pu.Mempool, ie.pu, gSysVariables)
	applyOverride(sess, ie.baseSessOpts)
	applyOverride(sess, opts)
	return sess
}

func (ie *internalExecutor) ApplySessionOverride(opts ie.SessionOverrideOptions) {
	ie.baseSessOpts = opts
}

type internalProtocol struct {
	database string
	username string
}

func (ip *internalProtocol) IsEstablished() bool {
	return true
}

func (ip *internalProtocol) SetEstablished() {}

func (ip *internalProtocol) GetRequest(payload []byte) *Request {
	panic("not impl")
}

// SendResponse sends a response to the client for the application request
func (ip *internalProtocol) SendResponse(resp *Response) error {
	return nil
}

// ConnectionID the identity of the client
func (ip *internalProtocol) ConnectionID() uint32 {
	return 74751101
}

// Peer gets the address [Host:Port] of the client
func (ip *internalProtocol) Peer() (string, string) {
	panic("not impl")
}

func (ip *internalProtocol) GetDatabaseName() string {
	return ip.database
}

func (ip *internalProtocol) SetDatabaseName(database string) {
	ip.database = database
}

func (ip *internalProtocol) GetUserName() string {
	return ip.username
}

func (ip *internalProtocol) SetUserName(username string) {
	ip.username = username
}

func (ip *internalProtocol) Quit() {}

//the server send group row of the result set as an independent packet thread safe
func (ip *internalProtocol) SendResultSetTextBatchRow(mrs *MysqlResultSet, cnt uint64) error {
	return nil
}

func (ip *internalProtocol) SendResultSetTextBatchRowSpeedup(mrs *MysqlResultSet, cnt uint64) error {
	return nil
}

//SendColumnDefinitionPacket the server send the column definition to the client
func (ip *internalProtocol) SendColumnDefinitionPacket(column Column, cmd int) error {
	return nil
}

//SendColumnCountPacket makes the column count packet
func (ip *internalProtocol) SendColumnCountPacket(count uint64) error {
	return nil
}

func (ip *internalProtocol) SendEOFPacketIf(warnings uint16, status uint16) error {
	return nil
}

//send OK packet to the client
func (ip *internalProtocol) sendOKPacket(affectedRows uint64, lastInsertId uint64, status uint16, warnings uint16, message string) error {
	return nil
}

//the OK or EOF packet thread safe
func (ip *internalProtocol) sendEOFOrOkPacket(warnings uint16, status uint16) error {
	return nil
}

func (ip *internalProtocol) PrepareBeforeProcessingResultSet() {
}

func (ip *internalProtocol) GetStats() string { return "internal unknown stats" }
