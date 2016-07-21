package service

import "encoding/json"

type ResponseService struct {
}

func (this *ResponseService) NewResponse(correlationId int32, responseMessage []byte) *Response {
	return &Response{CorrelationId: correlationId, ResponseMessage: responseMessage}
}

func (this *ResponseService) NewMetadaResponse(metadata map[string]TopicMetadata) (*Response, *Error) {
	bytes, err := json.Marshal(metadata)
	if err == nil {
		return this.NewResponse(1, bytes), nil
	}
	return nil, &Error{code: 2, msg: "Unable to serialize topicNames to json"}
}

func (this *ResponseService) NewProduceResponse(metadata map[string]TopicMetadata) (*Response, *Error) {
	bytes, err := json.Marshal(metadata)
	if err == nil {
		return this.NewResponse(1, bytes), nil
	}
	return nil, &Error{code: 2, msg: "Unable to serialize topicNames to json"}
}
