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
