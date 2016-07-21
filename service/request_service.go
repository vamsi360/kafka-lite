package service

import "encoding/json"

type RequestService struct {
}

func (this *RequestService) NewRequest(apiKey int16, apiVersion int16, correlationId int32, clientId string, requestMessage []byte) *Request {
	return &Request{apiKey: apiKey, apiVersion: apiVersion, correlationId: correlationId, clientId: clientId, requestMessage: requestMessage}
}

func (this *RequestService) NewMetadataRequest(clientId string, topicNames []string) (*Request, *Error) {
	bytes, err := json.Marshal(topicNames)
	if err == nil {
		return this.NewRequest(API_KEY_METADATA, 1, 1, clientId, bytes), nil
	}
	return nil, &Error{code: 2, msg: "Unable to serialize topicNames to json"}
}
