package models

type MQOption struct {
	Attributes map[string]string
}

type GetMQOption func(*MQOption) error

func WithAttributes(attributes map[string]string) GetMQOption {
	return func(opt *MQOption) error {
		opt.Attributes = attributes
		return nil
	}
}
