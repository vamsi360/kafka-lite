package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"

	"github.com/vamsi-subhash/kafka-lite/service"
	"github.com/vamsi-subhash/kafka-lite/storage"
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
		//log.Printf("=>Msg received: %v\n", inputRequest)

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
			producerSvc := storage.ProducerService{}
			produceResponse := producerSvc.ProduceMessage(&produceRequest)
			//fmt.Printf("ProduceResponse %+v\n", produceResponse)
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
			consumerSvc := storage.ConsumerService{}
			fetchResponse := consumerSvc.ConsumeMessage(&fetchRequest)
			//fmt.Printf("FetchResponse %+v\n", fetchResponse)
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
			//log.Printf("Wrote response bytes to the client: %s", string(responseBytes[:]))
		}
	}
}
