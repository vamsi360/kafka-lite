package producer

import "github.com/vamsi-subhash/kafka-lite/client/core"

type Accumulator struct {
	TopicAndPartitions *[]core.TopicAndPartition
	recordChannels     *map[TopicAndPartition]chan *core.ProducerRecord
}

func (this *Accumulator) append(record *core.ProducerRecord) {
	recordChannel := recordChannels[record.TopicAndPartition]
	if recordChannel == nil {

	}
}
