package server

type Request struct {
	//the command from the client
	cmd int

	//the data from the client
	data interface{}
}

func (req *Request) GetData() interface{} {
	return req.data
}

func (req *Request) SetData(data interface{}) {
	req.data = data
}

func (req *Request) GetCmd() int {
	return req.cmd
}

func (req *Request) SetCmd(cmd int) {
	req.cmd = cmd
}

type Response struct {
	//the category of the response
	category int

	//the status of executing the peer request
	status int

	//the command which generates the response
	cmd int

	//the data of the reponse
	data interface{}
}

const (
	// OK message
	okResponse = iota
	// Error message
	errorResponse
	// EOF message
	eofResponse
	//result message
	resultResponse
)

func (resp *Response) GetData() interface{} {
	return resp.data
}

func (resp *Response) SetData(data interface{}) {
	resp.data = data
}

func (resp *Response) GetStatus() int {
	return resp.status
}

func (resp *Response) SetStatus(status int) {
	resp.status = status
}

func (resp *Response) GetCategory() int {
	return resp.category
}

func (resp *Response) SetCategory(category int) {
	resp.category = category
}

type ClientProtocol interface {
	//the iteration between the client and the server to establish a working pipeline
	Handshake()error

	//the server reads a application request from the client
	ReadRequest()(*Request,error)

	//the server sends a response to the client for the application request
	SendResponse(*Response)(error)

	//get the host and port fo the client
	Peer()(string,string)

	//the identity of the client
	ConnectionID()uint32

	//close the protocol layer
	Close()
}

type ClientProtocolImpl struct{
	//io layer for the connection
	io IOPackage

	//random bytes
	salt []byte

	//the id of the connection
	connectionID uint32
}

func (cpi *ClientProtocolImpl) ConnectionID() uint32 {
	return cpi.connectionID
}

func (cpi *ClientProtocolImpl) Peer() (string, string) {
	return cpi.io.Peer()
}

func (cpi *ClientProtocolImpl) Close() {
	cpi.io.Close()
}