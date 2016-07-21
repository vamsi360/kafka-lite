package main

import (
	"fmt"

	"git.nm.flipkart.com/git/infra/kafka-lite/server"
	"git.nm.flipkart.com/git/infra/kafka-lite/service"
)

func createTestEntities() {
	requestSvc := service.RequestService{}

	topicNames := []string{"topic1", "topic2"}
	metadataReq, err := requestSvc.NewMetadataRequest("client123", topicNames)
	if err == nil {
		fmt.Println(metadataReq)
	}

	messageSvc := service.MessageService{}
	message := messageSvc.NewMessage(1, 1, 1, []byte("key"), []byte("msg"))
	fmt.Println(message)

	topic1 := service.Topic{Name: "topic1", NoOfPartitions: 1}
	partition1 := service.Partition{1, "first partition"}
	leaderNode := service.Node{1, "localhost", 9100}
	topicPartition1 := service.TopicPartition{Topic: topic1, Partition: partition1, LeaderNode: leaderNode, ReplicaNodes: []service.Node{}}

	responseSvc := service.ResponseService{}
	respMap := make(map[string]service.TopicMetadata)
	respMap["topic1"] = service.TopicMetadata{"topic1", []service.TopicPartition{topicPartition1}}
	metadataResp, err := responseSvc.NewMetadaResponse(respMap)
	if err == nil {
		fmt.Println(metadataResp)
	}
}

func main() {
	config := &server.ServerConfig{Host: "localhost", Port: 9100}
	fmt.Printf("== Starting server on port %d ==\n", config.Port)

	//createTestEntities()
	server := server.SocketServer{config}
	server.Start()

	fmt.Println("== Stopping server ==")
}
