package service

import "encoding/json"

type RequestService struct {
}

func (this *RequestService) NewRequest(apiKey int16, apiVersion int16, correlationId int32, clientId string, requestMessage []byte) *Request {
	return &Request{ApiKey: apiKey, ApiVersion: apiVersion, CorrelationId: correlationId, ClientId: clientId, RequestMessage: requestMessage}
}

func (this *RequestService) NewMetadataRequest(clientId string, topicNames []string) (*Request, *Error) {
	bytes, err := json.Marshal(topicNames)
	if err == nil {
		return this.NewRequest(API_KEY_METADATA, 1, 1, clientId, bytes), nil
	}
	return nil, &Error{code: 2, msg: "Unable to serialize topicNames to json"}
}

func (this *RequestService) NewProduceRequest(clientId string, requiredAcks int16, timeout int32, topicPartitionMessageSets *[]TopicPartitionMessageSet) (*Request, *Error) {
	produceRequest := ProduceRequest{RequiredAcks: requiredAcks, Timeout: timeout, TopicPartitionMessageSets: *topicPartitionMessageSets}
	bytes, err := json.Marshal(produceRequest)
	if err == nil {
		return this.NewRequest(API_KEY_PRODUCE, 1, 1, clientId, bytes), nil
	}
	return nil, &Error{code: 2, msg: "Unable to serialize topicNames to json"}
}

func (this *RequestService) NewFetchRequest(clientId string, replicaId int32, maxWaitTime int32, minBytes int32, topicPartitionOffsets *[]TopicPartitionOffset) (*Request, *Error) {
	fetchRequest := FetchRequest{ReplicaId: replicaId, MaxWaitTime: maxWaitTime, MinBytes: minBytes, TopicPartitionOffsets: *topicPartitionOffsets}
	bytes, err := json.Marshal(fetchRequest)
	if err == nil {
		return this.NewRequest(API_KEY_FETCH, 1, 1, clientId, bytes), nil
	}
	return nil, &Error{code: 2, msg: "Unable to serialize topicNames to json"}
}
