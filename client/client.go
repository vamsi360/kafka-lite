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
		//metadata request
		// topicNames := []string{"topic1", "topic2"}
		// request, err := requestSvc.NewMetadataRequest("client123", topicNames)
		// if err != nil {
		// 	log.Fatal("Error in creating metadata request")
		// }

		// bytes, jsonErr := json.Marshal(request)
		// if jsonErr != nil {
		// 	log.Fatal("Error in marshalling metadata request")
		// }

		topicName := "abc"
		partition := int32(0)

		messageSvc := service.MessageService{}
		message := messageSvc.NewMessage(1, 1, 1, []byte("key"), []byte("msg"))
		messageBytes, jsnErr := json.Marshal(message)
		if jsnErr != nil {
			log.Fatal("Error in converting msg to bytes")
		}
		var messageSize int32 = int32(len(messageBytes))
		messageAndOffset := service.MessageAndOffset{Offset: -1, MessageSize: messageSize, Message: *message}
		messageSet := service.MessageSet{MessageAndOffsets: []service.MessageAndOffset{messageAndOffset}}
		messageSetBytes, jsnErr := json.Marshal(messageSet)
		if jsnErr != nil {
			log.Fatal("Error in converting msg to bytes")
		}
		messageSetSize := int32(len(messageSetBytes))

		partitionMessageSet := service.PartitionMessageSet{Partition: partition, MessageSetSize: messageSetSize, MessageSet: messageSet}
		partitionMessageSets := []service.PartitionMessageSet{partitionMessageSet}

		topicPartitionMessageSet := service.TopicPartitionMessageSet{TopicName: topicName, PartitionMessageSets: partitionMessageSets}
		topicPartitionMessageSets := []service.TopicPartitionMessageSet{topicPartitionMessageSet}

		request, err := requestSvc.NewProduceRequest("client123", 1, 60000, &topicPartitionMessageSets)
		if err != nil {
			log.Fatal("Error in creating metadata request")
		}

		bytes, jsonErr := json.Marshal(request)
		if jsonErr != nil {
			log.Fatal("Error in marshalling metadata request")
		}

		log.Printf("Sending bytes: %s\n", string(bytes[:]))
		conn.Write(bytes)
		conn.Write([]byte("\n"))

		// // send to socket
		// fmt.Fprintf(conn, text+"\n")
		// listen for reply
		rmessage, _ := bufio.NewReader(conn).ReadString('\n')
		fmt.Print("Message from server: " + rmessage)

		time.Sleep(1 * time.Second)
	}
}
