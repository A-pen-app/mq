package models

import "time"

type ActionEvent struct {
	AppID     string     `json:"app_id"`
	AppUserID string     `json:"app_user_id"`
	Action    ActionType `json:"action"`
	ParamID   *string    `json:"param_id"`
}

type SvcActionEvent struct {
	Action      SvcActionType `json:"action"`
	PackageName string        `json:"package_name"`
	AppUserID   string        `json:"app_user_id"`
	ParamID     *string       `json:"param_id"`
	Reward      *Reward       `json:"reward"`
	Status      interface{}   `json:"status"`
	ExpiredAt   *time.Time    `json:"expired_at"`
}

type ActionType string

const (
	KeyPostCreate  ActionType = "post_create"
	KeyPostVote    ActionType = "post_vote"
	KeyPostShare   ActionType = "post_share"
	KeyPostComment ActionType = "post_comment"
	KeyPostBravo   ActionType = "post_bravo"
	KeyPostLike    ActionType = "post_like"
	KeyAppLaunch   ActionType = "app_launch"
)

type SvcActionType string

const (
	SvcKeyMegaphoneClear          SvcActionType = "megaphone_clear"
	SvcKeyFormRead                SvcActionType = "form_read"
	SvcKeyFormCompleted           SvcActionType = "form_completed"
	SvcKeyInvitationNewUserPassed SvcActionType = "invitation_new_user_passed"
	SvcKeyInvitationNewUserUsed   SvcActionType = "invitation_new_user_used"
	SvcKeyMissionCompleted        SvcActionType = "mission_completed"
	SvcKeyUserProductUpdated      SvcActionType = "user_product_updated"
	SvcKeyHireOnceProductConsumed SvcActionType = "hire_once_product_consumed"
	SvcKeyUserDeleted             SvcActionType = "user_deleted"
)

type Reward struct {
	Currency string `json:"currency"`
	Quantity string `json:"quantity"`
}
