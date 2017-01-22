package main

import (
	"bufio"
	"encoding/json"
	"log"
	"net"

	"github.com/vamsi-subhash/kafka-lite/service"
)

type Sender struct {
}

func (this *Sender) send(conn net.Conn, request *service.Request) *service.Response {
	bytes, jsonErr := json.Marshal(request)
	if jsonErr != nil {
		log.Fatal("Error in marshalling request")
	}

	// log.Printf("Sending bytes: %s\n", string(bytes[:]))
	conn.Write(bytes)
	conn.Write([]byte("\n"))

	respBytes, _ := bufio.NewReader(conn).ReadBytes('\n')
	// fmt.Print("Message from server: " + string(respBytes))

	var response service.Response
	jsonErr = json.Unmarshal(respBytes, &response)
	if jsonErr != nil {
		log.Fatal("Error in un-marshalling response", jsonErr)
	}

	return &response
}
