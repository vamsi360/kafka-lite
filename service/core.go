package service

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
	ApiKey         int16  `json:"apiKey"`
	ApiVersion     int16  `json:"apiVersion"`
	CorrelationId  int32  `json:"correlationId"`
	ClientId       string `json:"clientId"`
	RequestMessage []byte `json:"requestMessage"`
}

type Response struct {
	CorrelationId   int32  `json:"correlationId"`
	ResponseMessage []byte `json:"responseMessage"`
}

type Error struct {
	code int
	msg  string
}

type Message struct {
	crc        int32
	magicByte  int8
	attributes int8
	key        []byte
	value      []byte
}

type MessageSet struct {
	Offset      int64
	MessageSize int32
	Message     []Message
}

type Node struct {
	Id   int
	Host string
	Port int
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

type MetadataResponse struct {
	Metadata map[string]TopicMetadata
}
