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

	//the data of the reponse
	data interface{}
}

const (
	// OK message
	okResponse = iota
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

	//get the ip and port fo the client
	Peer()(string,int)
}

type ClientProtocolImpl struct{
	io IOPackage
	salt []byte

	//the id of the connection
	connectionID uint32
}

//TODO:implement it
func (cpi *ClientProtocolImpl) Peer() (string, int) {
	return "", 0
}