package producer

import (
	"git.nm.flipkart.com/git/infra/kafka-lite/client/core"
	"git.nm.flipkart.com/git/infra/kafka-lite/service"
)

type Accumulator struct {
	Topics        *[]service.Topic
	topicChannels map[string]chan *core.ProducerRecord
}

func (this *Accumulator) append(record *core.ProducerRecord) {

}
