package storage

import (
	"git.nm.flipkart.com/git/infra/kafka-lite/service"
	"os"
	"strconv"
	"bufio"
	"fmt"
)

const BaseDir  = "/var/kafka-lite/data"

var(
	handlers = make(map[string]map[int]bool)
)

func WriteMessage1(msg service.Message) {

}

func generateHandler(TopicName string, PartitionId int) {
	filePath := BaseDir + "/" + TopicName + "/" + strconv.Itoa(PartitionId) + "/" + strconv.Itoa(0)
	f, _ := os.OpenFile(filePath, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	handler := bufio.NewWriter(f)
	// read from persistant layer
	offset := 0
	position := 0
	for {
		fmt.Println("1")
		request := <-messageChan
		fmt.Println("2")
		for _, message := range request.Messages {
			offset += 1
			size, err := handler.Write(message)
			fmt.Println(err.Error(), size)
			position += size
		}
		f.Sync()
		*request.RespChan <- "done"
	}
}

func generateRoutine(TopicName string, PartitionId int) {
	if handlers[TopicName] == nil {
		handlers[TopicName] = make(map[int]bool)
	}
	if ! handlers[TopicName][PartitionId]{
		os.MkdirAll(BaseDir + "/" + TopicName + "/" + strconv.Itoa(PartitionId), 0666)
		go generateHandler(TopicName, PartitionId)
		handlers[TopicName][PartitionId] = true
	}
}

func getMeta() map[string][]int {
	m := make(map[string][]int)
	m["abc"] = []int{0}
	return m
}

func init() {
	go func() {
		for topic, partitions := range getMeta() {
			if partitions == nil {continue}
			for _, partition := range partitions {
				generateRoutine(topic, partition)
			}
		}

	}()
}

