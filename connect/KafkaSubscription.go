package connect

import (
	kafka "github.com/Shopify/sarama"
)

type KafkaSubscription struct {
	Topic    string
	GroupId  string
	Listener IKafkaMessageListener
	Handler  *kafka.ConsumerGroup
}
