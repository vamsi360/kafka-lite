package service

import "encoding/json"

type MessageService struct {
}

func (this *MessageService) NewMessage(crc int32, magicByte int8, attributes int8, key []byte, value []byte) *Message {
	return &Message{Crc: crc, MagicByte: magicByte, Attributes: attributes, Key: key, Value: value}
}

func (message *Message) SerializeJson() ([]byte, error) {
	if jsn, err := json.Marshal(message); err != nil {
		return make([]byte, 0), err
	} else {
		return jsn, err
	}
}
