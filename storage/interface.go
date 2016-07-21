package storage

import (
	"fmt"

	"git.nm.flipkart.com/git/infra/kafka-lite/service"
)

func WriteMessages(Topic string, PartitionId int, messages []*service.Message, respChan *chan *service.Response) (err error) {
	msg := MessageRequest{Messages: make([][]byte, len(messages)), RespChan: respChan}
	for idx, message := range messages {
		if bytes, err := message.SerializeJson(); err != nil {
			return err
		} else {
			msg.Messages[idx] = bytes
		}
	}
	fmt.Println("abc")
	messageChan <- msg
	return nil
}

func ReadMessages(Topic string, PartitionId int, offset int, count int) error {

	return nil
}
