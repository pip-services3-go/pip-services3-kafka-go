package build

import (
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	cbuild "github.com/pip-services3-go/pip-services3-components-go/build"
	connect "github.com/pip-services3-go/pip-services3-kafka-go/connect"
	queues "github.com/pip-services3-go/pip-services3-kafka-go/queues"
)

// Creates KafkaMessageQueue components by their descriptors.
// See KafkaMessageQueue
type DefaultKafkaFactory struct {
	*cbuild.Factory
}

// NewDefaultKafkaFactory method are create a new instance of the factory.
func NewDefaultKafkaFactory() *DefaultKafkaFactory {
	c := DefaultKafkaFactory{}
	c.Factory = cbuild.NewFactory()

	kafkaQueueFactoryDescriptor := cref.NewDescriptor("pip-services", "queue-factory", "kafka", "*", "1.0")
	kafkaConnectionDescriptor := cref.NewDescriptor("pip-services", "connection", "kafka", "*", "1.0")
	kafkaQueueDescriptor := cref.NewDescriptor("pip-services", "message-queue", "kafka", "*", "1.0")

	c.RegisterType(kafkaQueueFactoryDescriptor, NewKafkaMessageQueueFactory)

	c.RegisterType(kafkaConnectionDescriptor, connect.NewKafkaConnection)

	c.Register(kafkaQueueDescriptor, func(locator interface{}) interface{} {
		name := ""
		descriptor, ok := locator.(*cref.Descriptor)
		if ok {
			name = descriptor.Name()
		}

		return queues.NewKafkaMessageQueue(name)
	})

	return &c
}
