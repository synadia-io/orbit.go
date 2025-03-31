package natsstore

import "github.com/nats-io/nkeys"

func Secured(codec Codec, xkey nkeys.KeyPair) Codec {
	pk, _ := xkey.PublicKey()
	return &secureCodec{
		codec: codec,
		xkey:  xkey,
		pk:    pk,
	}
}

type secureCodec struct {
	codec Codec
	xkey  nkeys.KeyPair
	pk    string
}

func (s *secureCodec) Encode(a any) ([]byte, error) {
	b, err := s.codec.Encode(a)
	if err != nil {
		return nil, err
	}

	return s.xkey.Seal(b, s.pk)
}

func (s *secureCodec) Decode(bytes []byte, a any) error {
	b, err := s.xkey.Open(bytes, s.pk)
	if err != nil {
		return err
	}

	return s.codec.Decode(b, a)
}
