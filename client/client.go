package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	uuid "github.com/satori/go.uuid"

	"git.nm.flipkart.com/git/infra/kafka-lite/client/core"
	"git.nm.flipkart.com/git/infra/kafka-lite/service"
)

func main() {
	// connect to this socket
	conn, _ := net.Dial("tcp", "127.0.0.1:9100")
	sender := Sender{}
	metadataRequest := GetMetadataRequest(conn)
	metadataResponse := sender.send(conn, metadataRequest)

	offsetMap := make(map[core.TopicAndPartition]int64)

	responseMessage := metadataResponse.ResponseMessage
	var metadata map[string]service.TopicMetadata
	err := json.Unmarshal(responseMessage, &metadata)
	if err != nil {
		log.Println("Error in Metadata response un-marshalling")
	}
	log.Printf("Got Metadata Response\n")

	produceResponses := []service.Response{}
	fetchResponses := []service.Response{}
	count := 0
	for {
		produceRequest := GetProduceMessagesRequest(conn, metadata)
		produceResponse := sender.send(conn, produceRequest)
		produceResponses = append(produceResponses, *produceResponse)
		// log.Printf("Producer Response bytes size %d\n", len(produceResponse.ResponseMessage))

		fetchRequest := GetFetchMessagesRequest(conn, metadata, offsetMap)
		fetchResponse := sender.send(conn, fetchRequest)
		fetchResponses = append(fetchResponses, *fetchResponse)
		log.Printf("Fetch Response %s\n", string(fetchResponse.ResponseMessage[:]))

		count += 1
		time.Sleep(100 * time.Millisecond)
		if count%100 == 0 {
			log.Printf("%d produce&consume requests done", count)
			produceResponses = []service.Response{}
			fetchResponses = []service.Response{}
		}
	}
}

func GetMetadataRequest(conn net.Conn) *service.Request {
	topicNames := []string{"topic1", "topic2"}
	requestSvc := service.RequestService{}
	request, err := requestSvc.NewMetadataRequest("client123", topicNames)
	if err != nil {
		log.Fatal("Error in creating metadata request")
	}

	return request
}

func GetFetchMessagesRequest(conn net.Conn, metadata map[string]service.TopicMetadata, offsetMap map[core.TopicAndPartition]int64) *service.Request {
	topics := []service.Topic{}
	for topicName := range metadata {
		topicMetadata := metadata[topicName]
		partitions := int32(len(topicMetadata.TopicPartitions))
		topic := service.Topic{topicName, partitions}
		topics = append(topics, topic)
	}

	var topicPartitionOffsets []service.TopicPartitionOffset
	for _, topic := range topics {
		var partitionFetchOffsets []service.PartitionFetchOffset
		var i int32
		for i = 0; i < topic.NoOfPartitions; i++ {
			topicAndPartition := core.TopicAndPartition{Topic: topic.Name, Partition: int32(i)}
			offset, ok := offsetMap[topicAndPartition]
			if !ok {
				offset = 0
				offsetMap[topicAndPartition] = 0
			}
			partitionFetchOffset := service.PartitionFetchOffset{i, offset, 1024}
			offsetMap[topicAndPartition] = offset + 1
			partitionFetchOffsets = append(partitionFetchOffsets, partitionFetchOffset)
		}
		topicPartitionOffset := service.TopicPartitionOffset{TopicName: topic.Name, PartitionFetchOffsets: partitionFetchOffsets}
		topicPartitionOffsets = append(topicPartitionOffsets, topicPartitionOffset)
	}

	clientId := "client123"
	requestSvc := service.RequestService{}

	request, err := requestSvc.NewFetchRequest(clientId, -1, 10000, 1, &topicPartitionOffsets)
	if err != nil {
		log.Fatal("Error in creating request")
	}

	return request
}

func GetProduceMessagesRequest(conn net.Conn, metadata map[string]service.TopicMetadata) *service.Request {
	requestSvc := service.RequestService{}
	messageSvc := service.MessageService{}

	clientId := "client123"
	requiredAcks := int16(1)
	timeout := int32(60000)

	topics := []service.Topic{}
	for topicName := range metadata {
		topicMetadata := metadata[topicName]
		partitions := int32(len(topicMetadata.TopicPartitions))
		topic := service.Topic{topicName, partitions}
		topics = append(topics, topic)
	}

	var topicPartitionMessageSets []service.TopicPartitionMessageSet
	for _, topic := range topics {
		var partitionMessageSets []service.PartitionMessageSet
		var i int32
		for i = 0; i < topic.NoOfPartitions; i++ {
			//actual messages
			rstr := fmt.Sprintf("%s", uuid.NewV4())
			message := messageSvc.NewMessage(1, 1, 1, []byte(rstr), []byte(rstr))
			messages := []*service.Message{message}

			partitionMessageSet := NewPartitionMessageSet(topic.Name, i, messages)
			partitionMessageSets = append(partitionMessageSets, partitionMessageSet)
		}
		topicPartitionMessageSet := service.TopicPartitionMessageSet{TopicName: topic.Name, PartitionMessageSets: partitionMessageSets}
		topicPartitionMessageSets = append(topicPartitionMessageSets, topicPartitionMessageSet)
	}

	request, err := requestSvc.NewProduceRequest(clientId, requiredAcks, timeout, &topicPartitionMessageSets)
	if err != nil {
		log.Fatal("Error in creating request")
	}
	return request
}

func NewPartitionMessageSet(topicName string, partition int32, messages []*service.Message) service.PartitionMessageSet {
	var messageAndOffsets []service.MessageAndOffset
	for _, message := range messages {
		messageBytes, jsnErr := json.Marshal(message)
		if jsnErr != nil {
			log.Fatal("Error in converting msg to bytes")
		}
		var messageSize int32 = int32(len(messageBytes))
		messageAndOffset := service.MessageAndOffset{Offset: -1, MessageSize: messageSize, Message: *message}
		messageAndOffsets = append(messageAndOffsets, messageAndOffset)
	}

	messageSet := service.MessageSet{MessageAndOffsets: messageAndOffsets}
	messageSetBytes, jsnErr := json.Marshal(messageSet)
	if jsnErr != nil {
		log.Fatal("Error in converting msg to bytes")
	}
	messageSetSize := int32(len(messageSetBytes))
	return service.PartitionMessageSet{Partition: partition, MessageSetSize: messageSetSize, MessageSet: messageSet}
}
