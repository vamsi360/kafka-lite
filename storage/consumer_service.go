package storage

import (
	"encoding/json"
	"log"

	"git.nm.flipkart.com/git/infra/kafka-lite/service"
)

type ConsumerService struct {
}

func (this *ConsumerService) ConsumeMessage(fetchRequest *service.FetchRequest) *service.FetchResponse {
	// log.Printf("Received FetchReqeust for %d minBytes from replicaId %d\n", fetchRequest.MinBytes, fetchRequest.ReplicaId)

	topicPartitionFetchResponses := []service.TopicPartitionFetchResponse{}
	topicPartitionOffsets := fetchRequest.TopicPartitionOffsets
	for _, topicPartitionOffset := range topicPartitionOffsets {
		topicName := topicPartitionOffset.TopicName
		partitionFetchResponses := []service.PartitionFetchResponse{}
		partitionFetchOffsets := topicPartitionOffset.PartitionFetchOffsets
		for _, partitionFetchOffset := range partitionFetchOffsets {
			partition := partitionFetchOffset.Partition
			fetchOffset := partitionFetchOffset.FetchOffset
			maxBytes := partitionFetchOffset.MaxBytes
			//log.Printf("TopicName: %s; partition: %d; fetchOffset: %d; maxBytes: %d\n", topicName, partition, fetchOffset, maxBytes)

			storageService := StorageService{TopicName: topicName, Partition: partition}
			messageSet := storageService.ReadMessages(int(fetchOffset), int(maxBytes))
			//log.Printf("=>Read MessageSet: %+v\n", messageSet)
			bytes, err := json.Marshal(messageSet)
			if err != nil {
				log.Println("Error in marshalling messageSet")
				break
			}
			messageSetSize := int32(len(bytes))
			partitionFetchResponse := service.PartitionFetchResponse{Partition: partition, ErrorCode: -1, HighwaterMarkOffset: -1, MessageSetSize: messageSetSize, MessageSet: *messageSet}
			partitionFetchResponses = append(partitionFetchResponses, partitionFetchResponse)
		}

		topicPartitionFetchResponse := service.TopicPartitionFetchResponse{TopicName: topicName, PartitionFetchResponses: partitionFetchResponses}
		topicPartitionFetchResponses = append(topicPartitionFetchResponses, topicPartitionFetchResponse)
	}
	fetchResponse := service.FetchResponse{TopicPartitionFetchResponses: topicPartitionFetchResponses}
	return &fetchResponse
}
