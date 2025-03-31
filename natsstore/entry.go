package natsstore

func newEntry(codec Codec) Entry {
	return Entry{
		Revision: 0,
		codec:    codec,
	}
}

type Entry struct {
	Key      string
	Data     []byte
	Revision uint64
	codec    Codec
}

// IsNew returns true if the entry isn't stored
func (e Entry) IsNew() bool {
	return e.Revision == 0 && len(e.Data) == 0
}

// Decode uses the codec to decode the data in the entry yet
func (e Entry) Decode(a any) error {
	return e.codec.Decode(e.Data, a)
}

// Encode uses the codec to encode the data in the entry
func (e Entry) Encode(a any) ([]byte, error) {
	return e.codec.Encode(a)
}
