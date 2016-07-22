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
	Host           string `json:"host"`
	Port           int    `json:"port"`
	BrokerId       int    `json:"brokerId"`
	MaxMessageSize int    `json:"maxMessageSize"`
	LogDir         string `json:"logDir"`
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
			produceResponse := this.produceMessage(&produceRequest)
			fmt.Printf("ProduceResponse %+v\n", produceResponse)
			produceResponseBytes, _ := json.Marshal(produceResponse)
			responseService := &service.ResponseService{}
			response = responseService.NewResponse(inputRequest.CorrelationId, &produceResponseBytes)
		case service.API_KEY_FETCH:
			fetchRequestBytes := inputRequest.RequestMessage
			var fetchRequest service.FetchRequest
			fetchRequestErr := json.Unmarshal(fetchRequestBytes, &fetchRequest)
			if fetchRequestErr != nil {
				log.Println("Error in fetch request")
				break
			}
			fetchResponse := this.consumeMessage(&fetchRequest)
			fmt.Printf("FetchResponse %+v\n", fetchResponse)
			fetchResponseBytes, _ := json.Marshal(fetchResponse)
			responseService := &service.ResponseService{}
			response = responseService.NewResponse(inputRequest.CorrelationId, &fetchResponseBytes)
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

func (this *SocketServer) produceMessage(produceRequest *service.ProduceRequest) *service.ProduceResponse {
	log.Printf("Received ProduceRequest %+v\n", produceRequest)

	chanMap := make(map[string][]chan *service.PartitionProduceResponse)
	topicPartitionMessageSets := produceRequest.TopicPartitionMessageSets
	for _, topicPartitionMessageSet := range topicPartitionMessageSets {
		topicName := topicPartitionMessageSet.TopicName
		partitionMessageSets := topicPartitionMessageSet.PartitionMessageSets
		chanMap[topicName] = make([]chan *service.PartitionProduceResponse, len(topicPartitionMessageSet.PartitionMessageSets))
		for idx, partitionMessageSet := range partitionMessageSets {
			partition := partitionMessageSet.Partition
			messageSet := partitionMessageSet.MessageSet

			storageService := storage.Service{TopicName: topicName, Partition: partition}
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

func (this *SocketServer) consumeMessage(fetchRequest *service.FetchRequest) *service.FetchResponse {
	log.Printf("Received FetchReqeust %+v\n", fetchRequest)

	topicPartitionOffsets := fetchRequest.TopicPartitionOffsets
	for _, topicPartitionOffset := range topicPartitionOffsets {
		topicName := topicPartitionOffset.TopicName
		partitionFetchOffsets := topicPartitionOffset.PartitionFetchOffsets
		for _, partitionFetchOffset := range partitionFetchOffsets {
			partition := partitionFetchOffset.Partition
			fetchOffset := partitionFetchOffset.FetchOffset
			maxBytes := partitionFetchOffset.MaxBytes
			log.Printf("TopicName: %s; partition: %d; fetchOffset: %d; maxBytes: %d\n", topicName, partition, fetchOffset, maxBytes)

		}
	}
	fetchResponse := service.FetchResponse{}
	return &fetchResponse
}
