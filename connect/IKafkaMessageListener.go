package connect

import (
	kafka "github.com/Shopify/sarama"
)

type IKafkaMessageListener interface {
	kafka.ConsumerGroupHandler
}
