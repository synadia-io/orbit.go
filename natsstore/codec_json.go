package natsstore

import "encoding/json"

func NewJsonCodec() Codec {
	return &jsonCodec{}
}

type jsonCodec struct{}

func (c *jsonCodec) Encode(a any) ([]byte, error) {
	return json.Marshal(a)
}

func (c *jsonCodec) Decode(data []byte, a any) error {
	return json.Unmarshal(data, a)
}
