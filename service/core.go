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
	Crc        int32  `json:"crc"`
	MagicByte  int8   `json:"magicByte"`
	Attributes int8   `json:"attributes"`
	Key        []byte `json:"key"`
	Value      []byte `json:"value"`
}

type MessageAndOffset struct {
	Offset      int64   `json:"offset"`
	MessageSize int32   `json:"messageSize"`
	Message     Message `json:"message"`
}

type MessageSet struct {
	MessageAndOffsets []MessageAndOffset `json:"messageSet"`
}

type Node struct {
	Id   int    `json:"id"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

type Topic struct {
	Name           string `json:"name"`
	NoOfPartitions int32  `json:"partitions"`
}

type Partition struct {
	Id   int    `json:"id"`
	Desc string `json:"desc"`
}

type TopicPartition struct {
	Topic        Topic     `json:"topic"`
	Partition    Partition `json:"partition"`
	LeaderNode   Node      `json:"leaderNode"`
	ReplicaNodes []Node    `json:"replicaNodes"`
}

type TopicMetadata struct {
	TopicName       string           `json:"topicName"`
	TopicPartitions []TopicPartition `json:"topicPartitions"`
}

type MetadataRequest struct {
	TopicNames []string `json:"topicNames"`
}

type MetadataResponse struct {
	Metadata map[string]TopicMetadata `json:"metadata"`
}

type PartitionMessageSet struct {
	Partition      int32      `json:"partition"`
	MessageSetSize int32      `json:"messageSetSize"`
	MessageSet     MessageSet `json:"messageSet"`
}

type TopicPartitionMessageSet struct {
	TopicName            string                `json:"topicName"`
	PartitionMessageSets []PartitionMessageSet `json:"partitionMessageSets"`
}

type ProduceRequest struct {
	RequiredAcks              int16                      `json:"requiredAcks"`
	Timeout                   int32                      `json:"timeout"`
	TopicPartitionMessageSets []TopicPartitionMessageSet `json:"topicPartitionMessageSets"`
}

type PartitionProduceResponse struct {
	Partition int32 `json:"partition"`
	ErrorCode int16 `json:"errorCode"`
	Offset    int64 `json:"offset"`
}

type TopicPartitionProduceResponse struct {
	TopicName                 string                      `json:"topicName"`
	PartitionProduceResponses []*PartitionProduceResponse `json:"partitionProduceResponses"`
}

type ProduceResponse struct {
	TopicPartitionProduceResponses []*TopicPartitionProduceResponse `json:"topicPartitionProduceResponses"`
}

type PartitionFetchOffset struct {
	Partition   int32 `json:"partition"`
	FetchOffset int64 `json:"fetchOffset"`
	MaxBytes    int32 `json:"maxBytes"`
}

type TopicPartitionOffset struct {
	TopicName             string                 `json:"topicName"`
	PartitionFetchOffsets []PartitionFetchOffset `json:"partitionFetchOffset"`
}

type FetchRequest struct {
	ReplicaId             int32                  `json:"replicaId"`
	MaxWaitTime           int32                  `json:"maxWaitTime"`
	MinBytes              int32                  `json:"minBytes"`
	TopicPartitionOffsets []TopicPartitionOffset `json:"topicPartitionOffsets"`
}

type PartitionFetchResponse struct {
	Partition           int32      `json:"partition"`
	ErrorCode           int16      `json:"errorCode"`
	HighwaterMarkOffset int64      `json:"highwaterMarkOffset"`
	MessageSetSize      int32      `json:"messageSetSize"`
	MessageSet          MessageSet `json:"messageSet"`
}

type TopicPartitionFetchResponse struct {
	TopicName               string                   `json:"topicName"`
	PartitionFetchResponses []PartitionFetchResponse `json:"partitionFetchResponse"`
}

type FetchResponse struct {
	TopicPartitionFetchResponses []TopicPartitionFetchResponse `json:"topicPartitionFetchResponses"`
}
