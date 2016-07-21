package main

import "fmt"
import "git.nm.flipkart.com/git/infra/kafka-lite/service"

func main() {
	fmt.Println("== Starting server ==")

	topicNames := []string{"topic1", "topic2"}
	requestSvc := service.RequestService{}
	metadataReq, err := requestSvc.NewMetadataRequest("client123", topicNames)
	if err == nil {
		fmt.Println(metadataReq)
	}
}
