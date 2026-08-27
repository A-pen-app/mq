package pubsub

import (
	"testing"

	"github.com/A-pen-app/mq/v2/models"
)

func TestParseTopicPath(t *testing.T) {
	cases := []struct {
		key            string
		project, topic string
		ok             bool
	}{
		{"svc-action", "", "", false},
		{"projects/penpeer-production/topics/svc-action", "penpeer-production", "svc-action", true},
		{models.TopicPath("penpeer-production", models.TopicSvcAction), "penpeer-production", "svc-action", true},
		{"projects//topics/svc-action", "", "", false},
		{"projects/p/subscriptions/s", "", "", false},
		{"projects/p/topics/", "", "", false},
	}
	for _, c := range cases {
		project, topic, ok := parseTopicPath(c.key)
		if ok != c.ok || project != c.project || topic != c.topic {
			t.Errorf("parseTopicPath(%q) = (%q, %q, %v), want (%q, %q, %v)", c.key, project, topic, ok, c.project, c.topic, c.ok)
		}
	}
}
