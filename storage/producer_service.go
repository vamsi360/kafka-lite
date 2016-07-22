package storage

import (
	"git.nm.flipkart.com/git/infra/kafka-lite/service"
)

type ProducerService struct {
}

func (this *ProducerService) ProduceMessage(produceRequest *service.ProduceRequest) *service.ProduceResponse {
	// log.Printf("Received ProduceRequest with requiredAcks %d\n", produceRequest.RequiredAcks)
	chanMap := make(map[string][]chan *service.PartitionProduceResponse)
	topicPartitionMessageSets := produceRequest.TopicPartitionMessageSets
	for _, topicPartitionMessageSet := range topicPartitionMessageSets {
		topicName := topicPartitionMessageSet.TopicName
		partitionMessageSets := topicPartitionMessageSet.PartitionMessageSets
		chanMap[topicName] = make([]chan *service.PartitionProduceResponse, len(topicPartitionMessageSet.PartitionMessageSets))
		for idx, partitionMessageSet := range partitionMessageSets {
			partition := partitionMessageSet.Partition
			messageSet := partitionMessageSet.MessageSet

			storageService := StorageService{TopicName: topicName, Partition: partition}
			respChan := make(chan *service.PartitionProduceResponse)
			storageService.WriteMessages(&messageSet, &respChan)
			chanMap[topicName][idx] = respChan
		}
	}
	produceResponse := service.ProduceResponse{make([]*service.TopicPartitionProduceResponse, 0, len(topicPartitionMessageSets))}
	for topic, chans := range chanMap {
		topicPartitionProduceResponse := &service.TopicPartitionProduceResponse{topic, make([]*service.PartitionProduceResponse, len(chans))}
		for idx, ch := range chans {
			topicPartitionProduceResponse.PartitionProduceResponses[idx] = <-ch
		}
		produceResponse.TopicPartitionProduceResponses = append(produceResponse.TopicPartitionProduceResponses, topicPartitionProduceResponse)
	}
	return &produceResponse
}
