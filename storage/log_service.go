package storage

import (
	"encoding/json"
	"os"
	"strconv"

	"io/ioutil"

	"git.nm.flipkart.com/git/infra/kafka-lite/service"
)

const BaseDir = "/tmp/kafka-lite/data"

var (
	handlers        = make(map[string]map[int32]bool)
	indexMap        = make(map[string]map[int32]map[string]int)
	currentOffset   = make(map[string]map[int32]int64)
	currentPosition = make(map[string]map[int32]int)
)

func readIndex(TopicName string, PartitionId int32) (err error) {
	if indexMap[TopicName] == nil {
		indexMap[TopicName] = make(map[int32]map[string]int)
		currentOffset[TopicName] = make(map[int32]int64)
		currentPosition[TopicName] = make(map[int32]int)
	}
	if indexMap[TopicName][PartitionId] == nil {
		indexMap[TopicName][PartitionId] = make(map[string]int)
		currentOffset[TopicName][PartitionId] = int64(0)
		currentPosition[TopicName][PartitionId] = 0
		filePath := BaseDir + "/" + TopicName + "/" + strconv.Itoa(int(PartitionId)) + "/" + strconv.Itoa(0)
		if bytes, err := ioutil.ReadFile(filePath + ".index"); err == nil {
			var idx map[string]int
			json.Unmarshal(bytes, &idx)
			indexMap[TopicName][PartitionId] = idx
			currentPosition[TopicName][PartitionId] = len(bytes)
		}
		if bytes, err := ioutil.ReadFile(filePath + ".offset"); err == nil {
			var idx int64
			json.Unmarshal(bytes, &idx)
			currentOffset[TopicName][PartitionId] = idx
		}

	}
	return nil
}

func logWriter(TopicName string, PartitionId int32) {
	defer offsetWriter(TopicName, PartitionId)
	filePath := BaseDir + "/" + TopicName + "/" + strconv.Itoa(int(PartitionId)) + "/" + strconv.Itoa(0) + ".log"
	f, _ := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	messageChan := messageChan(TopicName, PartitionId)
	filePath = BaseDir + "/" + TopicName + "/" + strconv.Itoa(int(PartitionId)) + "/" + strconv.Itoa(0) + ".index"
	idxfd, _ := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	// read from persistant layer
	for {
		request := <-messageChan
		response := service.PartitionProduceResponse{Partition: PartitionId, ErrorCode: 0, Offset: currentOffset[TopicName][PartitionId]}
		for _, message := range request.Messages {
			currentOffset[TopicName][PartitionId] += 1
			size, _ := f.Write(message)
			currentPosition[TopicName][PartitionId] += size
			indexMap[TopicName][PartitionId][strconv.Itoa(int(currentOffset[TopicName][PartitionId]-1))] = currentPosition[TopicName][PartitionId]
		}
		b, _ := json.Marshal(indexMap[TopicName][PartitionId])
		idxfd.WriteAt(b, 0)
		//if currentPosition[TopicName][PartitionId] % 10 == 0 {
		//	offsetWriter(TopicName, PartitionId)
		//}
		*request.RespChan <- &response
	}
}

func logReader(TopicName string, PartitionId int32, offset int, maxBytes int) *service.MessageSet {
	initPos := indexMap[TopicName][PartitionId][strconv.Itoa(offset)]
	filePath := BaseDir + "/" + TopicName + "/" + strconv.Itoa(int(PartitionId)) + "/" + strconv.Itoa(0) + ".log"
	fd, _ := os.Open(filePath)
	index := offset

	messageAndOffsets := []service.MessageAndOffset{}
	for finPos := initPos; finPos < initPos+maxBytes; {
		index += 1
		nextPos := indexMap[TopicName][PartitionId][strconv.Itoa(index)]
		if nextPos == 0 {
			break
		}
		size := nextPos - finPos
		//log.Printf("nextPos: %d; finPos: %d; size: %d\n", nextPos, finPos, size)
		b := make([]byte, size)
		fd.ReadAt(b, int64(finPos))

		var message service.Message
		json.Unmarshal(b, &message)

		messageAndOffset := service.MessageAndOffset{Offset: int64(currentOffset[TopicName][PartitionId]), MessageSize: int32(size), Message: message}
		messageAndOffsets = append(messageAndOffsets, messageAndOffset)

		finPos = nextPos
	}
	messageSet := service.MessageSet{MessageAndOffsets: messageAndOffsets}
	return &messageSet
}

func offsetWriter(TopicName string, PartitionId int32) {
	filePath := BaseDir + "/" + TopicName + "/" + strconv.Itoa(int(PartitionId)) + "/" + strconv.Itoa(0) + ".offset"
	offsetfd, _ := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	b, _ := json.Marshal(currentOffset[TopicName][PartitionId])
	offsetfd.WriteAt(b, 0)
}

func generateRoutines(TopicName string, PartitionId int32) {
	if handlers[TopicName] == nil {
		handlers[TopicName] = make(map[int32]bool)
	}

	if !handlers[TopicName][PartitionId] {
		os.MkdirAll(BaseDir+"/"+TopicName+"/"+strconv.Itoa(int(PartitionId)), 0777)
		messageChanMap[TopicName] = make(map[int32](chan MessageRequest))
		messageChanMap[TopicName][PartitionId] = make(chan MessageRequest)
		readIndex(TopicName, PartitionId)
		go logWriter(TopicName, PartitionId)
		go offsetWriter(TopicName, PartitionId)
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
				generateRoutines(topic, partition)
			}
		}

	}()
}
