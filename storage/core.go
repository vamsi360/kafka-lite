package storage


type MessageRequest struct {
	Messages [][]byte
	RespChan *chan string
}

var messageChan = make(chan MessageRequest)
