package build

import (
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	"github.com/pip-services3-go/pip-services3-kafka-go/queues"
	"github.com/pip-services3-go/pip-services3-messaging-go/build"
	cqueues "github.com/pip-services3-go/pip-services3-messaging-go/queues"
)

// KafkaMessageQueueFactory are creates KafkaMessageQueue components by their descriptors.
// Name of created message queue is taken from its descriptor.
//
// See Factory
// See KafkaMessageQueue
type KafkaMessageQueueFactory struct {
	build.MessageQueueFactory
}

// NewKafkaMessageQueueFactory method are create a new instance of the factory.
func NewKafkaMessageQueueFactory() *KafkaMessageQueueFactory {
	c := KafkaMessageQueueFactory{
		MessageQueueFactory: *build.InheritMessageQueueFactory(),
	}

	kafkaQueueDescriptor := cref.NewDescriptor("pip-services", "message-queue", "kafka", "*", "1.0")

	c.Register(kafkaQueueDescriptor, func(locator interface{}) interface{} {
		name := ""
		descriptor, ok := locator.(*cref.Descriptor)
		if ok {
			name = descriptor.Name()
		}
		return c.CreateQueue(name)
	})

	return &c
}

// Creates a message queue component and assigns its name.
//
// Parameters:
//   - name: a name of the created message queue.
func (c *KafkaMessageQueueFactory) CreateQueue(name string) cqueues.IMessageQueue {
	queue := queues.NewKafkaMessageQueue(name)

	if c.Config != nil {
		queue.Configure(c.Config)
	}
	if c.References != nil {
		queue.SetReferences(c.References)
	}

	return queue
}
