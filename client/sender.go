package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"

	"git.nm.flipkart.com/git/infra/kafka-lite/service"
)

type Sender struct {
}

func (this *Sender) send(conn net.Conn, request *service.Request) *service.Response {
	bytes, jsonErr := json.Marshal(request)
	if jsonErr != nil {
		log.Fatal("Error in marshalling request")
	}

	log.Printf("Sending bytes: %s\n", string(bytes[:]))
	conn.Write(bytes)
	conn.Write([]byte("\n"))

	rmessage, _ := bufio.NewReader(conn).ReadString('\n')
	fmt.Print("Message from server: " + rmessage)

	return nil
}
