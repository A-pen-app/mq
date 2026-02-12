package models

type DeploymentEnvironmentType string

const (
	DeploymentEnvironmentTypeProduction  DeploymentEnvironmentType = "production"
	DeploymentEnvironmentTypeDevelopment DeploymentEnvironmentType = "development"
)

type ReceiverType string

const (
	ReceiverTypeApen      ReceiverType = "apen"
	ReceiverTypeNurse     ReceiverType = "nurse"
	ReceiverTypePhar      ReceiverType = "phar"
	ReceiverTypeMegaphone ReceiverType = "megaphone"
	ReceiverTypeShop      ReceiverType = "shop"
	ReceiverTypeDelibird  ReceiverType = "delibird"
	ReceiverTypeMeetup    ReceiverType = "meetup"
)

const (
	TopicSvcAction            string = "svc-action"
	TopicAction               string = "action"
	TopicWandererDev          string = "wanderer-dev"
	TopicWandererProd         string = "wanderer-prod"
	TopicStickiness           string = "stickiness"
	TopicNotificationSegments string = "notification-segments"
	TopicMail                 string = "mail"
	TopicNotif                string = "notif"
)

type MQOption struct {
	Attributes *Attributes
}

type Attributes struct {
	Receiver              ReceiverType              `json:"receiver"`
	DeploymentEnvironment DeploymentEnvironmentType `json:"deployment_environment"`
}

type GetMQOption func(*MQOption) error

func WithAttributes(attributes *Attributes) GetMQOption {
	return func(opt *MQOption) error {
		opt.Attributes = attributes
		return nil
	}
}

type Message struct {
	Data     []byte
	AckFunc  func()
	NackFunc func()
}

func (m *Message) Ack() {
	if m.AckFunc != nil {
		m.AckFunc()
	}
}

func (m *Message) Nack() {
	if m.NackFunc != nil {
		m.NackFunc()
	}
}
