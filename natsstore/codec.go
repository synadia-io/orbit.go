package natsstore

// Codec provides a way to encode and decode data
type Codec interface {
    Encode(any) ([]byte, error)
    Decode([]byte, any) error
}
