package storage

import "git.nm.flipkart.com/git/infra/kafka-lite/service"

type Service struct {
	TopicName string
	Partition int32
}

func (service *Service) WriteMessages(messageSet *service.MessageSet, respChan *chan *service.PartitionProduceResponse) (err error) {
	msg := MessageRequest{Messages: make([][]byte, len(messageSet.MessageAndOffsets)), RespChan: respChan}
	for idx, messageAndOffset := range messageSet.MessageAndOffsets {
		if bytes, err := messageAndOffset.Message.SerializeJson(); err != nil {
			return err
		} else {
			msg.Messages[idx] = bytes
		}
	}
	messageChan(service.TopicName, service.Partition) <- msg
	return nil
}

func (service *Service) ReadMessages(offset int, count int) error {

	return nil
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
