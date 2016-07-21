package service

import "encoding/json"

type ResponseService struct {
}

func (this *ResponseService) NewResponse(correlationId int32, responseMessage []byte) *Response {
	return &Response{correlationId: correlationId, responseMessage: responseMessage}
}

func (this *ResponseService) NewMetadaResponse(metadata *map[string]TopicMetadata) *Response {
	bytes, err := json.Marshal(metadata)
	if err != nil {
		return this.NewResponse(1, bytes)
	}
	return nil
}
