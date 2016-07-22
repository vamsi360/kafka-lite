package main

import (
	"encoding/json"
	"log"
	"net"
	"time"

	"git.nm.flipkart.com/git/infra/kafka-lite/service"
)

func main() {
	// connect to this socket
	conn, _ := net.Dial("tcp", "127.0.0.1:9100")
	sender := Sender{}
	for {
		metadataRequest := GetMetadataRequest(conn)
		metadataResponse := sender.send(conn, metadataRequest)

		responseMessage := metadataResponse.ResponseMessage
		var metadata map[string]service.TopicMetadata
		err := json.Unmarshal(responseMessage, &metadata)
		if err != nil {
			log.Println("Error in Metadata response un-marshalling")
		}
		log.Printf("MetadataResp: %+v\n", metadata)

		produceRequest := GetProduceMessagesRequest(conn, metadata)
		produceResponse := sender.send(conn, produceRequest)
		log.Printf("Producer Response %+v\n", produceResponse)

		fetchRequest := GetFetchMessagesRequest(conn, metadata)
		fetchResponse := sender.send(conn, fetchRequest)
		log.Printf("Fetch Response %+v\n", fetchResponse)

		time.Sleep(1 * time.Second)
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

func GetFetchMessagesRequest(conn net.Conn, metadata map[string]service.TopicMetadata) *service.Request {
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
			partitionFetchOffset := service.PartitionFetchOffset{i, 0, 10240}
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
	log.Printf("Request %v\n", request)

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
			message := messageSvc.NewMessage(1, 1, 1, []byte("key"), []byte("msg"))
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
	log.Printf("Request %v\n", request)

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
