package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"runtime"

	"git.nm.flipkart.com/git/infra/kafka-lite/server"
	"git.nm.flipkart.com/git/infra/kafka-lite/service"
)

func createTestEntities() {
	requestSvc := service.RequestService{}
	topicNames := []string{"topic1", "topic2"}
	metadataReq, err := requestSvc.NewMetadataRequest("client123", topicNames)
	if err == nil {
		fmt.Printf("MetadataReq: %v\n", metadataReq)
		reqBytes, _ := json.Marshal(*metadataReq)
		fmt.Printf("MetadataReqJson: %s\n", string(reqBytes[:]))
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

	// messages := []*service.Message{message}
	// ch := make(chan string, 1)
	// storage.WriteMessages("abc", 0, messages, &ch)
	// <-ch
}

func main() {
	runtime.GOMAXPROCS(1)

	config := &server.ServerConfig{}
	bytes, err := ioutil.ReadFile("serverconfig.json")
	if err != nil {
		log.Fatal("Error in reading config")
	}
	err = json.Unmarshal(bytes, config)
	if err != nil {
		log.Fatal("Error in decoding json config")
	}

	fmt.Printf("ServerConfig: %+v\n", config)
	fmt.Printf("== Starting server on port %d ==\n", config.Port)
	defer fmt.Println("== Stopping server ==")

	//createTestEntities()
	server := server.SocketServer{config}
	server.Start()

}
