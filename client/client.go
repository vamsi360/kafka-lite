package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"git.nm.flipkart.com/git/infra/kafka-lite/service"
)

func main() {
	// connect to this socket
	conn, _ := net.Dial("tcp", "127.0.0.1:9100")
	for {
		// // read in input from stdin
		// reader := bufio.NewReader(os.Stdin)
		// fmt.Print("Text to send: ")
		// text, _ := reader.ReadString('\n')

		requestSvc := service.RequestService{}
		topicNames := []string{"topic1", "topic2"}
		metadataReq, err := requestSvc.NewMetadataRequest("client123", topicNames)
		if err != nil {
			log.Fatal("Error in creating metadata request")
		}

		bytes, jsonErr := json.Marshal(metadataReq)
		if jsonErr != nil {
			log.Fatal("Error in marshalling metadata request")
		}

		log.Printf("Sending bytes: %s\n", string(bytes[:]))
		conn.Write(bytes)
		conn.Write([]byte("\n"))
		// // send to socket
		// fmt.Fprintf(conn, text+"\n")
		// listen for reply
		message, _ := bufio.NewReader(conn).ReadString('\n')
		fmt.Print("Message from server: " + message)

		time.Sleep(1 * time.Second)
	}
}
