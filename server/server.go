package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"

	"git.nm.flipkart.com/git/infra/kafka-lite/service"
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
