package core

type TopicAndPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

type ProducerRecord struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
}

type RecordMetadata struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Checksum  int32  `json:"checksum"`
}
