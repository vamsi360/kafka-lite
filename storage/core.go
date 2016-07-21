package storage

import "git.nm.flipkart.com/git/infra/kafka-lite/service"

type MessageRequest struct {
	Messages [][]byte
	RespChan *chan *service.Response
}

var messageChan = make(chan MessageRequest)
