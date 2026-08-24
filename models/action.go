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
	SvcKeyMegaphoneClear                SvcActionType = "megaphone_clear"
	SvcKeyFormRead                      SvcActionType = "form_read"
	SvcKeyFormCompleted                 SvcActionType = "form_completed"
	SvcKeyInvitationNewUserPassed       SvcActionType = "invitation_new_user_passed"
	SvcKeyInvitationNewUserUsed         SvcActionType = "invitation_new_user_used"
	SvcKeyMissionCompleted              SvcActionType = "mission_completed"
	SvcKeyUserProductUpdated            SvcActionType = "user_product_updated"
	SvcKeyHireOnceProductConsumed       SvcActionType = "hire_once_product_consumed"
	SvcKeyUserDeleted                   SvcActionType = "user_deleted"
	SvcKeyHireSubscriptionPauseReminder SvcActionType = "hire_subscription_pause_reminder"
	SvcKeyHireSubscriptionAutoResumed   SvcActionType = "hire_subscription_auto_resumed"
	// 帳號憑證在 App 端被改動。事件不帶新值——收件者拿 AppUserID 去讀平台當下的
	// 那一筆，亂序或重送都會收斂到同一個結果。
	SvcKeyPasswordChanged SvcActionType = "password_changed"
	SvcKeyEmailVerified   SvcActionType = "email_verified"
)

type Reward struct {
	Currency string `json:"currency"`
	Quantity string `json:"quantity"`
}

type ConsumeType string

const (
	AttendMeetup ConsumeType = "attend_meetup"
)

type ConsumeEvent struct {
	AppID       string      `json:"app_id" binding:"required"`
	AppUserID   string      `json:"app_user_id" binding:"required"`
	ConsumeType ConsumeType `json:"consume_type" binding:"required"`
	ParamId     *string     `json:"param_id"`
	Awards      int         `json:"awards"`
	Coins       int         `json:"coins"`
}
