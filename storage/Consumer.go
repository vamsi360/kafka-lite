package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"git.nm.flipkart.com/git/infra/kafka-lite/service"
)

const BaseDir = "/tmp/kafka-lite/data"

var (
	handlers = make(map[string]map[int32]bool)
)

func generateHandler(TopicName string, PartitionId int32) {
	filePath := BaseDir + "/" + TopicName + "/" + strconv.Itoa(int(PartitionId)) + "/" + strconv.Itoa(0)
	f, _ := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	handler := bufio.NewWriter(f)
	messageChan := messageChan(TopicName, PartitionId)
	// read from persistant layer
	offset := int64(0)
	position := 0
	for {
		fmt.Println("1")
		request := <-messageChan
		fmt.Println("2")
		response := service.PartitionProduceResponse{Partition: PartitionId, ErrorCode: 0, Offset: offset}
		for _, message := range request.Messages {
			offset += 1
			size, err := handler.Write(message)
			position += size
			fmt.Println(string(message), size, err, offset, position, handler.Buffered())
		}
		fmt.Println(handler.Flush())
		fmt.Println(f.Sync())

		//construct response object
		*request.RespChan <- &response
	}
}

func generateRoutine(TopicName string, PartitionId int32) {
	if handlers[TopicName] == nil {
		handlers[TopicName] = make(map[int32]bool)
	}

	if !handlers[TopicName][PartitionId] {
		os.MkdirAll(BaseDir+"/"+TopicName+"/"+strconv.Itoa(int(PartitionId)), 0777)
		messageChanMap[TopicName] = make(map[int32](chan MessageRequest))
		messageChanMap[TopicName][PartitionId] = make(chan MessageRequest)
		go generateHandler(TopicName, PartitionId)
		handlers[TopicName][PartitionId] = true
	}
}

func getMeta() map[string][]int32 {
	m := make(map[string][]int32)
	nodeId := 1
	metadataService := service.MetadataService{}
	response := metadataService.GetMetadataResponse()
	if response == nil {
		panic("unable to get metadata")
	}
	var topicMetadataMap map[string]service.TopicMetadata
	json.Unmarshal(response.ResponseMessage, &topicMetadataMap)
	for topicName, topicMetadata := range topicMetadataMap {
		m[topicName] = make([]int32, 0, len(topicMetadata.TopicPartitions))
		for _, topicPartition := range topicMetadata.TopicPartitions {
			if topicPartition.LeaderNode.Id == nodeId {
				m[topicName] = append(m[topicName], int32(topicPartition.Partition.Id))
			}
		}
	}
	log.Println(m)
	return m
}

func init() {
	go func() {
		// Ideally Listen on changes to Meta and Run. Keeping  a routine for that reason
		for topic, partitions := range getMeta() {
			if partitions == nil {
				continue
			}
			for _, partition := range partitions {
				generateRoutine(topic, partition)
			}
		}

	}()
}
