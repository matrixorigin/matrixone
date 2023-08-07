package adapter

type MessageQueue interface {
	Connect() error
	Publish(topic string, message []byte) error
	Subscribe(topic string) (<-chan []byte, error)
	Close() error
}
