package test_queues

import (
	"testing"
	"time"

	"github.com/pip-services3-go/pip-services3-messaging-go/queues"
	"github.com/stretchr/testify/assert"
)

type MessageQueueFixture struct {
	queue queues.IMessageQueue
}

func NewMessageQueueFixture(queue queues.IMessageQueue) *MessageQueueFixture {
	c := MessageQueueFixture{
		queue: queue,
	}
	return &c
}

func (c *MessageQueueFixture) TestSendReceiveMessage(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))
	sndErr := c.queue.Send("", envelope1)
	assert.Nil(t, sndErr)

	time.Sleep(100 * time.Millisecond)

	// if c.queue.GetCapabilities().CanMessageCount() {
	// 	count, rdErr := c.queue.MessageCount()
	// 	assert.Nil(t, rdErr)
	// 	assert.Greater(t, count, (int64)(0))
	// }

	envelope2, rcvErr := c.queue.Receive("", 10000*time.Millisecond)
	assert.Nil(t, rcvErr)
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)
}

func (c *MessageQueueFixture) TestMessageCount(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))
	c.queue.Send("", envelope1)

	time.Sleep(500 * time.Millisecond)

	count, err := c.queue.ReadMessageCount()
	assert.Nil(t, err)
	assert.True(t, count >= 1)
}

func (c *MessageQueueFixture) TestReceiveSendMessage(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))

	time.AfterFunc(500*time.Millisecond, func() {
		c.queue.Send("", envelope1)
	})

	envelope2, rcvErr := c.queue.Receive("", 10000*time.Millisecond)
	assert.Nil(t, rcvErr)
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)
}

func (c *MessageQueueFixture) TestReceiveCompleteMessage(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))
	sndErr := c.queue.Send("", envelope1)
	assert.Nil(t, sndErr)

	time.Sleep(100 * time.Millisecond)

	// count, rdErr := c.queue.ReadMessageCount()
	// assert.Nil(t, rdErr)
	// assert.Greater(t, count, (int64)(0))

	envelope2, rcvErr := c.queue.Receive("", 10000*time.Millisecond)
	assert.Nil(t, rcvErr)
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)

	cplErr := c.queue.Complete(envelope2)
	assert.Nil(t, cplErr)
	assert.Nil(t, envelope2.GetReference())
}

func (c *MessageQueueFixture) TestReceiveAbandonMessage(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))
	sndErr := c.queue.Send("", envelope1)
	assert.Nil(t, sndErr)

	envelope2, rcvErr := c.queue.Receive("", 10000*time.Millisecond)
	assert.Nil(t, rcvErr)
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)

	abdErr := c.queue.Abandon(envelope2)
	assert.Nil(t, abdErr)

	envelope2, rcvErr = c.queue.Receive("", 10000*time.Millisecond)
	assert.Nil(t, rcvErr)
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)
}

func (c *MessageQueueFixture) TestSendPeekMessage(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))
	sndErr := c.queue.Send("", envelope1)
	assert.Nil(t, sndErr)

	time.Sleep(1000 * time.Millisecond)

	envelope2, pkErr := c.queue.Peek("")
	assert.Nil(t, pkErr)
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)

	// pop message from queue for next test
	_, rcvErr := c.queue.Receive("", 10000*time.Millisecond)
	assert.Nil(t, rcvErr)
}

func (c *MessageQueueFixture) TestPeekNoMessage(t *testing.T) {
	envelope, pkErr := c.queue.Peek("")
	assert.Nil(t, pkErr)
	assert.Nil(t, envelope)
}

func (c *MessageQueueFixture) TestMoveToDeadMessage(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))
	sndErr := c.queue.Send("", envelope1)
	assert.Nil(t, sndErr)

	envelope2, rcvErr := c.queue.Receive("", 10000*time.Millisecond)
	assert.Nil(t, rcvErr)
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)

	mvErr := c.queue.MoveToDeadLetter(envelope2)
	assert.Nil(t, mvErr)
}

func (c *MessageQueueFixture) TestOnMessage(t *testing.T) {
	envelope1 := queues.NewMessageEnvelope("123", "Test", []byte("Test message"))
	receiver := &TestMsgReceiver{}
	c.queue.BeginListen("", receiver)

	time.Sleep(1000 * time.Millisecond)

	sndErr := c.queue.Send("", envelope1)
	assert.Nil(t, sndErr)

	time.Sleep(1000 * time.Millisecond)

	envelope2 := receiver.envelope
	assert.NotNil(t, envelope2)
	assert.Equal(t, envelope1.MessageType, envelope2.MessageType)
	assert.Equal(t, envelope1.Message, envelope2.Message)
	assert.Equal(t, envelope1.CorrelationId, envelope2.CorrelationId)

	c.queue.EndListen("")
}

type TestMsgReceiver struct {
	envelope *queues.MessageEnvelope
}

func (c *TestMsgReceiver) ReceiveMessage(envelope *queues.MessageEnvelope, queue queues.IMessageQueue) (err error) {
	c.envelope = envelope
	return nil
}
