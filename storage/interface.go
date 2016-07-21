package storage

import (
	"git.nm.flipkart.com/git/infra/kafka-lite/service"
	"fmt"
)


func WriteMessages(Topic string, PartitionId int, messages []*service.Message, respChan *chan string ) (err error) {
	msg := MessageRequest{Messages:make([][]byte, len(messages)), RespChan:respChan}
	for idx, message := range messages {
		if bytes, err := message.SerializeJson() ; err != nil{
			msg.Messages[idx] = bytes
		}
		return err
	}
	fmt.Println("abc")
	messageChan <- msg
	return nil
}

func ReadMessages(Topic string, PartitionId int, offset int, count int ) error {

	return nil
}