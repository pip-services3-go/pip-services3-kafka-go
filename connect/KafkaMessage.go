package connect

import (
	kafka "github.com/Shopify/sarama"
)

type KafkaMessage struct {
	Message *kafka.ConsumerMessage
	Session kafka.ConsumerGroupSession
}
