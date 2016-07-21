package core

import (
	"encoding/json"

	"github.com/satori/go.uuid"
)

const (
	API_KEY_PRODUCE             = 0
	API_KEY_FETCH               = 1
	API_KEY_METADATA            = 3
	API_KEY_UPDATE_METADATA_KEY = 6
)

const (
	ERROR                  = 1
	METADATA_REQUEST_ERROR = 2
)

type Request struct {
	ApiKey         int16
	ApiVersion     int16
	CorrelationId  int32
	ClientId       string
	RequestMessage []byte
}

type Response struct {
	CorrelationId   int32
	ResponseMessage []byte
}

type Error struct {
	Code int
	Msg  string
}

type Message struct {
	Crc        int32
	MagicByte  int8
	Attributes int8
	Key        []byte
	Value      []byte
}

type MessageSet struct {
	Offset      int64
	MessageSize int32
	Message     []Message
}

type Node struct {
	Id   int
	Host string
	Port string
}

type Topic struct {
	Name           string
	NoOfPartitions int
}

type Partition struct {
	Id   int
	Desc string
}

type TopicPartition struct {
	Topic        Topic
	Partition    Partition
	LeaderNode   Node
	ReplicaNodes []Node
}

type TopicMetadata struct {
	TopicName       string
	TopicPartitions []TopicPartition
}

type MetadataRequest struct {
	TopicNames []string
}

func NewMetadataRequest(clientId string, topicNames *[]string) *Request {
	bytes, err := json.Marshal(topicNames)
	if err != nil {
		return Request{ApiKey: API_KEY_METATDATA, ApiVersion: 1, CorrelationId: uuid.NewV4(), ClientId: clientId, RequestMessage: bytes}
	}
	return Error{Code: 2, Msg: "Unable to serialize topicNames"}
}

type MetadataResponse struct {
	Metadata map[string]TopicMetadata
}

func NewMetadaResponse(metadata *map[string]TopicMetadata) *Response {
	bytes, err := json.Marshal(metadata)
	if err != nil {
		return Response{CorrelationId: uuid.NewV4(), ResponseMessage: bytes}
	}
	return Error{Code: 2, Msg: "Unable to serialize topicNames"}
}
