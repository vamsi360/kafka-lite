package core

type TopicAndPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

type ProducerRecord struct {
	TopicAndPartition TopicAndPartition `json:"topicAndPartition"`
	Key               []byte            `json:"key"`
	Value             []byte            `json:"value"`
}

type RecordMetadata struct {
	TopicAndPartition TopicAndPartition `json:"topicAndPartition"`
	Offset            int64             `json:"offset"`
	Checksum          int32             `json:"checksum"`
}
