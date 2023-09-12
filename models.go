package mq

type Mail struct {
	From        string   `json:"from"`
	FromAddress string   `json:"from_address"`
	Name        []string `json:"name"`
	Address     []string `json:"to"`
	Subject     string   `json:"subject"`
	Data        string   `json:"data"`
}

type AppNotification struct {
	To           []string          `json:"to"`
	Data         map[string]string `json:"data"`
	Notification FcmNotification   `json:"notification"`
	Silent       bool              `json:"silent"`
}

// FCMNotification ...
type FcmNotification struct {
	BodyLocKey  string   `json:"body_loc_key"`
	BodyLocArgs []string `json:"body_loc_args"`
	Badge       int      `json:"badge"`
}
