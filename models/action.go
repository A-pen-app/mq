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
	// On hire_subscription_pause_reminder; ExpiredAt is then the auto-resume time.
	PausedAt *time.Time `json:"paused_at,omitempty"`
	// On the hire_order_* actions; ParamID is the order id.
	Order *Order `json:"order"`
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
	SvcKeyCredentialChanged             SvcActionType = "credential_changed"
	// A web hire order moved; shop publishes, medgo mails the vendor. The
	// transfer pair is ATM-only; paid covers both methods (Order.Method).
	SvcKeyHireOrderAwaitingTransfer SvcActionType = "hire_order_awaiting_transfer"
	SvcKeyHireOrderPaid             SvcActionType = "hire_order_paid"
	SvcKeyHireOrderTransferExpired  SvcActionType = "hire_order_transfer_expired"
	// A recruiter paused or resumed hiring themselves, or the subscription is
	// about to expire; shop publishes, medgo mails the vendor.
	SvcKeyHireSubscriptionPaused   SvcActionType = "hire_subscription_paused"
	SvcKeyHireSubscriptionResumed  SvcActionType = "hire_subscription_resumed"
	SvcKeyHireSubscriptionExpiring SvcActionType = "hire_subscription_expiring"
	// The hot feed's intent survey card (apen-api, PROD-170). apen tells shop
	// two facts, with ParamID the Amplitude survey id on both: served, the
	// card first went out to the user; completed, the user answered it. Shop
	// answers every completed with rewarded, Status one of the
	// IntentSurveyRewardStatus values, Reward set when coins were granted.
	SvcKeyIntentSurveyServed    SvcActionType = "intent_survey_served"
	SvcKeyIntentSurveyCompleted SvcActionType = "intent_survey_completed"
	SvcKeyIntentSurveyRewarded  SvcActionType = "intent_survey_rewarded"
)

// IntentSurveyRewardStatus is the Status of an intent_survey_rewarded event:
// what shop did with one completed report. Every value is final; the sender
// never reports the same completion again once answered.
type IntentSurveyRewardStatus string

const (
	IntentSurveyRewardGranted    IntentSurveyRewardStatus = "granted"     // coins granted by this report; Reward says how many
	IntentSurveyRewardAlready    IntentSurveyRewardStatus = "already"     // granted by an earlier report of the same completion
	IntentSurveyRewardCapReached IntentSurveyRewardStatus = "cap_reached" // the mission's total cap is spent
	// no live mission is bound to this survey: none was ever created, or the
	// one there is was ended or deleted, is inactive, expired or not started
	// yet, or belongs to another app.
	IntentSurveyRewardNoMission IntentSurveyRewardStatus = "no_mission"
)

type Reward struct {
	Currency string `json:"currency"`
	Quantity string `json:"quantity"`
}

type PaymentMethod string

const (
	PaymentMethodATM    PaymentMethod = "atm"
	PaymentMethodCredit PaymentMethod = "credit"
)

// Order is what the mail needs that the event does not already carry —
// the platform is PackageName, the id ParamID, the deadline ExpiredAt.
type Order struct {
	Months    int           `json:"months"`
	Quantity  int           `json:"quantity"`
	AmountTWD int           `json:"amount_twd"`
	Method    PaymentMethod `json:"method"`

	// Set on hire_order_awaiting_transfer.
	BankCode       string `json:"bank_code,omitempty"`
	VirtualAccount string `json:"virtual_account,omitempty"`

	// Set on hire_order_paid. Queued marks a renewal that is paid but waits
	// for the live subscription to lapse before it starts, which is also why
	// a queued order carries no StartsAt/EndsAt: it has no dates yet.
	PaidAt   *time.Time `json:"paid_at,omitempty"`
	Queued   bool       `json:"queued,omitempty"`
	StartsAt *time.Time `json:"starts_at,omitempty"`
	EndsAt   *time.Time `json:"ends_at,omitempty"`

	// When the buyer placed the order. The cancellation mail shows it.
	CreatedAt *time.Time `json:"created_at,omitempty"`
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
