package service

type MessageService struct {
}

func (this *MessageService) NewMessage(crc int32, magicByte int8, attributes int8, key []byte, value []byte) *Message {
	return &Message{crc: crc, magicByte: magicByte, attributes: attributes, key: key, value: value}
}
