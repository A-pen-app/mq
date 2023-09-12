package config

var IsProd = GetBool("PRODUCTION_ENVIRONMENT")
var TopicAction = GetString("TOPIC_ACTION")
var RabbitmqConnURL = GetString("RABBITMQ_CONN_URL")
