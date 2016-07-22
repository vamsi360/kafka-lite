package storage

import (
	"git.nm.flipkart.com/git/infra/kafka-lite/service"
)

type StorageService struct {
	TopicName string
	Partition int32
}

func (this *StorageService) WriteMessages(messageSet *service.MessageSet, respChan *chan *service.PartitionProduceResponse) (err error) {
	msg := MessageRequest{Messages: make([][]byte, len(messageSet.MessageAndOffsets)), RespChan: respChan}
	for idx, messageAndOffset := range messageSet.MessageAndOffsets {
		if bytes, err := messageAndOffset.Message.SerializeJson(); err != nil {
			return err
		} else {
			msg.Messages[idx] = bytes
		}
	}
	messageChan(this.TopicName, this.Partition) <- msg
	return nil
}

func (this *StorageService) ReadMessages(offset int, maxBytes int) *service.MessageSet {
	messages := logReader(this.TopicName, this.Partition, offset, maxBytes)

	messageAndOffsets := []service.MessageAndOffset{}
	for _, message := range *messages {
		messageAndOffset := service.MessageAndOffset{Message: message}
		messageAndOffsets = append(messageAndOffsets, messageAndOffset)
	}
	messageSet := service.MessageSet{MessageAndOffsets: messageAndOffsets}

	return &messageSet
}

type MessageRequest struct {
	Messages [][]byte
	RespChan *chan *service.PartitionProduceResponse
}

var messageChanMap = make(map[string]map[int32](chan MessageRequest))

func messageChan(TopicName string, PartitionId int32) chan MessageRequest {
	if messageChanMap[TopicName] == nil {
		messageChanMap[TopicName] = make(map[int32](chan MessageRequest))
	}
	if messageChanMap[TopicName][PartitionId] == nil {
		messageChanMap[TopicName][PartitionId] = make(chan MessageRequest)
	}
	return messageChanMap[TopicName][PartitionId]
}
