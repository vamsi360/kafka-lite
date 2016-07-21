package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"

	"git.nm.flipkart.com/git/infra/kafka-lite/service"
	"git.nm.flipkart.com/git/infra/kafka-lite/storage"
)

const (
	REQUEST_DELIM = '\n'
)

type ServerConfig struct {
	Host string
	Port int
}

type SocketServer struct {
	Config *ServerConfig
}

func (this *SocketServer) Start() {
	log.Println("== Starting Socket Server ==")
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", this.Config.Port))
	if err != nil {
		log.Fatal("Unable to start the socket server", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("Unable to checkout connection", err)
		}
		go this.handleConnection(conn)
	}
}

func (this *SocketServer) handleConnection(conn net.Conn) {
	defer func() {
		if conn != nil {
			log.Println("Closing the open connection from client", conn.RemoteAddr)
			conn.Close()
		}
	}()

	for {
		var delim byte = byte(REQUEST_DELIM)
		bytes, err := bufio.NewReader(conn).ReadBytes(delim)
		if err != nil {
			log.Println("Error in reading bytes - ignoring - please retry", err)
			break
		}
		var inputRequest service.Request
		err = json.Unmarshal(bytes, &inputRequest)
		if err != nil {
			log.Println("Error in unmarshalling input request", err)
		}
		log.Printf("=>Msg received: %v\n", inputRequest)

		var response *service.Response

		apiKey := inputRequest.ApiKey
		switch apiKey {
		case service.API_KEY_METADATA:
			metadataSvc := service.MetadataService{}
			response = metadataSvc.GetMetadataResponse()
		case service.API_KEY_PRODUCE:
			requestMessageBytes := inputRequest.RequestMessage
			var produceRequest service.ProduceRequest
			produceReqErr := json.Unmarshal(requestMessageBytes, &produceRequest)
			if produceReqErr != nil {
				log.Println("Error in produce request")
				break
			}
			response = this.produceMessage(&produceRequest)
		default:
			log.Printf("Un-supported apiKey %d - returning nil\n", apiKey)
			response = nil
		}

		responseBytes, respErr := json.Marshal(response)
		if respErr == nil {
			conn.Write(responseBytes)
			conn.Write([]byte("\n"))
			log.Printf("Wrote response bytes to the client: %s", string(responseBytes[:]))
		}
	}
}

func (this *SocketServer) produceMessage(produceRequest *service.ProduceRequest) *service.Response {
	log.Printf("Received ProduceRequest %v\n", produceRequest)

	topicPartitionMessageSets := produceRequest.TopicPartitionMessageSets
	for _, topicPartitionMessageSet := range topicPartitionMessageSets {
		topicName := topicPartitionMessageSet.TopicName
		partitionMessageSets := topicPartitionMessageSet.PartitionMessageSets
		for _, partitionMessageSet := range partitionMessageSets {
			partition := partitionMessageSet.Partition
			messageSet := partitionMessageSet.MessageSet
			for _, messageAndOffset := range messageSet.MessageAndOffsets {
				log.Printf("topic: %s; partition: %d, offset: %d; messageSize: %d; message: %+v\n",
					topicName, partition, messageAndOffset.Offset, messageAndOffset.MessageSize, messageAndOffset.Message)
			}
		}
	}

	topicName := "abc"
	partitionId := 0
	var messages []*service.Message = []*service.Message{}
	respChan := make(chan *service.Response)
	// write message to storage layer
	storage.WriteMessages(topicName, partitionId, messages, &respChan)
	return <-respChan
}
