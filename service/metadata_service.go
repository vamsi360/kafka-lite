package service

import (
	"log"
)

type MetadataService struct {
}

func (this *MetadataService) GetMetadataResponse() *Response {
	log.Println("Request for metadata")

	topic1 := Topic{Name: "abc", NoOfPartitions: 1}
	partition1 := Partition{0, "first partition"}
	leaderNode := Node{1, "localhost", 9100}
	topicPartition1 := TopicPartition{Topic: topic1, Partition: partition1, LeaderNode: leaderNode, ReplicaNodes: []Node{}}

	responseSvc := ResponseService{}
	respMap := make(map[string]TopicMetadata)
	respMap[topic1.Name] = TopicMetadata{topic1.Name, []TopicPartition{topicPartition1}}
	metadataResp, err := responseSvc.NewMetadaResponse(respMap)
	if err == nil {
		log.Printf("MetatadataResponseMap %+v\n", respMap)
		return metadataResp
	}
	return nil
}
