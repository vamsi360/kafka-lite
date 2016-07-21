package service

import (
	"log"
)

type MetadataService struct {
}

func (this *MetadataService) GetMetadataResponse() *Response {
	log.Println("Request for metadata")

	topic1 := Topic{Name: "topic1", NoOfPartitions: 2}
	topic2 := Topic{Name: "topic2", NoOfPartitions: 2}
	partition1 := Partition{0, "first partition"}
	partition2 := Partition{1, "second partition"}
	leaderNode := Node{1, "localhost", 9100}
	topicPartition1 := TopicPartition{Topic: topic1, Partition: partition1, LeaderNode: leaderNode, ReplicaNodes: []Node{}}
	topicPartition2 := TopicPartition{Topic: topic1, Partition: partition2, LeaderNode: leaderNode, ReplicaNodes: []Node{}}
	topicPartition3 := TopicPartition{Topic: topic2, Partition: partition1, LeaderNode: leaderNode, ReplicaNodes: []Node{}}
	topicPartition4 := TopicPartition{Topic: topic2, Partition: partition2, LeaderNode: leaderNode, ReplicaNodes: []Node{}}

	responseSvc := ResponseService{}
	respMap := make(map[string]TopicMetadata)
	respMap[topic1.Name] = TopicMetadata{topic1.Name, []TopicPartition{topicPartition1, topicPartition2}}
	respMap[topic2.Name] = TopicMetadata{topic2.Name, []TopicPartition{topicPartition3, topicPartition4}}
	metadataResp, err := responseSvc.NewMetadaResponse(respMap)
	if err == nil {
		log.Printf("MetatadataResponseMap %+v\n", respMap)
		return metadataResp
	}
	return nil
}
