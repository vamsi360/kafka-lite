package storage

import (
	"github.com/vamsi-subhash/kafka-lite/service"
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
	return logReader(this.TopicName, this.Partition, offset, maxBytes)
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
