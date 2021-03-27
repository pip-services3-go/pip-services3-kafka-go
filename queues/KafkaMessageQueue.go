package queues

import (
	"fmt"
	"time"

	kafka "github.com/Shopify/sarama"
	cconf "github.com/pip-services3-go/pip-services3-commons-go/config"
	cerr "github.com/pip-services3-go/pip-services3-commons-go/errors"
	cref "github.com/pip-services3-go/pip-services3-commons-go/refer"
	clog "github.com/pip-services3-go/pip-services3-components-go/log"
	connect "github.com/pip-services3-go/pip-services3-kafka-go/connect"
	cqueues "github.com/pip-services3-go/pip-services3-messaging-go/queues"
)

/*
KafkaMessageQueue are message queue that sends and receives messages via Kafka message broker.

 Configuration parameters:

- topic:                         name of Kafka topic to subscribe
- group_id:                      (optional) consumer group id (default: default)
- from_beginning:                (optional) restarts receiving messages from the beginning (default: false)
- read_partitions:               (optional) number of partitions to be consumed concurrently (default: 1)
- autocommit:                    (optional) turns on/off autocommit (default: true)
- connection(s):
  - discovery_key:               (optional) a key to retrieve the connection from  IDiscovery
  - host:                        host name or IP address
  - port:                        port number
  - uri:                         resource URI or connection string with all parameters in it
- credential(s):
  - store_key:                   (optional) a key to retrieve the credentials from  ICredentialStore
  - username:                    user name
  - password:                    user password
- options:
  - autosubscribe:        (optional) true to automatically subscribe on option (default: false)
  - acks                  (optional) control the number of required acks: -1 - all, 0 - none, 1 - only leader (default: -1)
  - log_level:            (optional) log level 0 - None, 1 - Error, 2 - Warn, 3 - Info, 4 - Debug (default: 1)
  - connect_timeout:      (optional) number of milliseconds to connect to broker (default: 1000)
  - max_retries:          (optional) maximum retry attempts (default: 5)
  - retry_timeout:        (optional) number of milliseconds to wait on each reconnection attempt (default: 30000)
  - request_timeout:      (optional) number of milliseconds to wait on flushing messages (default: 30000)


 References:

- *:logger:*:*:1.0             (optional)  ILogger components to pass log messages
- *:counters:*:*:1.0           (optional)  ICounters components to pass collected measurements
- *:discovery:*:*:1.0          (optional)  IDiscovery services to resolve connections
- *:credential-store:*:*:1.0   (optional) Credential stores to resolve credentials
- *:connection:kafka:*:1.0      (optional) Shared connection to Kafka service

See MessageQueue
See MessagingCapabilities

Example:

    queue := NewKafkaMessageQueue("myqueue")
    queue.Configure(cconf.NewConfigParamsFromTuples(
      "subject", "mytopic",
      "connection.protocol", "kafka"
      "connection.host", "localhost"
      "connection.port", 1883
    ))

    queue.open("123")

    queue.Send("123", NewMessageEnvelope("", "mymessage", "ABC"))

    message, err := queue.Receive("123")
	if (message != nil) {
		...
		queue.Complete("123", message);
	}
*/
type KafkaMessageQueue struct {
	cqueues.MessageQueue

	defaultConfig   *cconf.ConfigParams
	config          *cconf.ConfigParams
	references      cref.IReferences
	opened          bool
	localConnection bool

	//The dependency resolver.
	DependencyResolver *cref.DependencyResolver
	//The logger.
	Logger *clog.CompositeLogger
	//The Kafka connection component.
	Connection *connect.KafkaConnection

	topic          string
	groupId        string
	fromBeginning  bool
	autoCommit     bool
	readPartitions int
	acks           int
	autoSubscribe  bool
	subscribed     bool
	messages       []cqueues.MessageEnvelope
	receiver       cqueues.IMessageReceiver
}

// Creates a new instance of the queue component.
//   - name    (optional) a queue name.
func NewKafkaMessageQueue(name string) *KafkaMessageQueue {
	c := KafkaMessageQueue{
		defaultConfig: cconf.NewConfigParamsFromTuples(
			"topic", nil,
			"group_id", "default",
			"from_beginning", false,
			"read_partitions", 1,
			"autocommit", true,
			"options.autosubscribe", false,
			"options.acks", -1,
			"options.log_level", 1,
			"options.connect_timeout", 1000,
			"options.retry_timeout", 30000,
			"options.max_retries", 5,
			"options.request_timeout", 30000,
		),
		Logger: clog.NewCompositeLogger(),
	}
	c.MessageQueue = *cqueues.InheritMessageQueue(&c, name,
		cqueues.NewMessagingCapabilities(false, true, true, true, true, false, false, false, true))
	c.DependencyResolver = cref.NewDependencyResolver()
	c.DependencyResolver.Configure(c.defaultConfig)

	c.messages = make([]cqueues.MessageEnvelope, 0)

	return &c
}

// Configures component by passing configuration parameters.
//   - config    configuration parameters to be set.
func (c *KafkaMessageQueue) Configure(config *cconf.ConfigParams) {
	config = config.SetDefaults(c.defaultConfig)
	c.config = config

	c.DependencyResolver.Configure(config)

	c.topic = config.GetAsStringWithDefault("topic", c.topic)
	c.groupId = config.GetAsStringWithDefault("group_id", c.groupId)
	c.fromBeginning = config.GetAsBooleanWithDefault("from_beginning", c.fromBeginning)
	c.readPartitions = config.GetAsIntegerWithDefault("read_partitions", c.readPartitions)
	c.autoCommit = config.GetAsBooleanWithDefault("autocommit", c.autoCommit)
	c.autoSubscribe = config.GetAsBooleanWithDefault("options.autosubscribe", c.autoSubscribe)
	c.acks = config.GetAsIntegerWithDefault("options.acks", c.acks)
}

// Sets references to dependent components.
//   - references 	references to locate the component dependencies.
func (c *KafkaMessageQueue) SetReferences(references cref.IReferences) {
	c.references = references
	c.Logger.SetReferences(references)

	// Get connection
	c.DependencyResolver.SetReferences(references)
	result := c.DependencyResolver.GetOneOptional("connection")
	if dep, ok := result.(*connect.KafkaConnection); ok {
		c.Connection = dep
	}
	// Or create a local one
	if c.Connection == nil {
		c.Connection = c.createConnection()
		c.localConnection = true
	} else {
		c.localConnection = false
	}
}

// Unsets (clears) previously set references to dependent components.
func (c *KafkaMessageQueue) UnsetReferences() {
	c.Connection = nil
}

func (c *KafkaMessageQueue) createConnection() *connect.KafkaConnection {
	connection := connect.NewKafkaConnection()
	if c.config != nil {
		connection.Configure(c.config)
	}
	if c.references != nil {
		connection.SetReferences(c.references)
	}
	return connection
}

// Checks if the component is opened.
// Returns true if the component has been opened and false otherwise.
func (c *KafkaMessageQueue) IsOpen() bool {
	return c.opened
}

// Opens the component.
//   - correlationId 	(optional) transaction id to trace execution through call chain.
//   - Returns 			 error or nil no errors occured.
func (c *KafkaMessageQueue) Open(correlationId string) (err error) {
	if c.opened {
		return nil
	}

	if c.Connection == nil {
		c.Connection = c.createConnection()
		c.localConnection = true
	}

	if c.localConnection {
		err = c.Connection.Open(correlationId)
	}

	if err == nil && c.Connection == nil {
		err = cerr.NewInvalidStateError(correlationId, "NO_CONNECTION", "Kafka connection is missing")
	}

	if err == nil && !c.Connection.IsOpen() {
		err = cerr.NewConnectionError(correlationId, "CONNECT_FAILED", "Kafka connection is not opened")
	}

	if err != nil {
		return err
	}

	// Automatically subscribe if needed
	if c.autoSubscribe {
		err = c.subscribe(correlationId)
		if err != nil {
			return err
		}
	}

	c.opened = true

	return err
}

// Closes component and frees used resources.
//   - correlationId 	(optional) transaction id to trace execution through call chain.
//   - Returns 			error or nil no errors occured.
func (c *KafkaMessageQueue) Close(correlationId string) (err error) {
	if !c.opened {
		return nil
	}

	if c.Connection == nil {
		return cerr.NewInvalidStateError(correlationId, "NO_CONNECTION", "Kafka connection is missing")
	}

	if c.localConnection {
		err = c.Connection.Close(correlationId)
	}
	if err != nil {
		return err
	}

	// Unsubscribe from topic
	if c.subscribed {
		topic := c.getTopic()
		c.Connection.Unsubscribe(topic, c.groupId, c)
		c.subscribed = false
	}

	c.Lock.Lock()
	defer c.Lock.Unlock()
	c.opened = false
	c.receiver = nil
	c.messages = make([]cqueues.MessageEnvelope, 0)

	return nil
}

func (c *KafkaMessageQueue) getTopic() string {
	if c.topic != "" {
		return c.topic
	}
	return c.Name()
}

func (c *KafkaMessageQueue) subscribe(correlationId string) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	// Check if already were subscribed
	if c.subscribed {
		return nil
	}

	// Subscribe to the topic
	topic := c.getTopic()
	config := kafka.NewConfig()
	config.Consumer.Offsets.AutoCommit.Enable = c.autoCommit
	err := c.Connection.Subscribe(topic, c.groupId, config, c)
	if err != nil {
		c.Logger.Error(correlationId, err, "Failed to subscribe to topic "+topic)
		return err
	}

	c.subscribed = true
	return nil
}

func (c *KafkaMessageQueue) fromMessage(message *cqueues.MessageEnvelope) (*kafka.ProducerMessage, error) {
	if message == nil {
		return nil, nil
	}

	headers := []kafka.RecordHeader{
		{
			Key:   []byte("correlation_id"),
			Value: []byte(message.CorrelationId),
		},
		{
			Key:   []byte("message_type"),
			Value: []byte(message.MessageType),
		},
	}

	msg := &kafka.ProducerMessage{}
	msg.Topic = c.getTopic()
	msg.Key = kafka.StringEncoder(message.MessageId)
	msg.Value = kafka.ByteEncoder(message.Message)
	msg.Headers = headers

	msg.Timestamp = time.Now()

	return msg, nil
}

func (c *KafkaMessageQueue) toMessage(msg *connect.KafkaMessage) (*cqueues.MessageEnvelope, error) {
	message := cqueues.NewEmptyMessageEnvelope()

	messageType := c.getHeaderByKey(msg.Message.Headers, "message_type")
	correlationId := c.getHeaderByKey(msg.Message.Headers, "correlation_id")

	message = cqueues.NewMessageEnvelope(correlationId, messageType, nil)
	message.MessageId = string(msg.Message.Key)
	message.SentTime = msg.Message.Timestamp
	message.Message = msg.Message.Value
	message.SetReference(msg)

	return message, nil
}

func (c *KafkaMessageQueue) getHeaderByKey(headers []*kafka.RecordHeader, key string) string {
	for _, header := range headers {
		if key == string(header.Key) {
			return string(header.Value)
		}
	}
	return ""
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *KafkaMessageQueue) Setup(kafka.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *KafkaMessageQueue) Cleanup(kafka.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *KafkaMessageQueue) ConsumeClaim(session kafka.ConsumerGroupSession, claim kafka.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for msg := range claim.Messages() {
		message := &connect.KafkaMessage{
			Message: msg,
			Session: session,
		}

		c.OnMessage(message)

		if c.autoCommit {
			session.MarkMessage(msg, "")
			session.Commit()
		}
	}

	return nil
}

func (c *KafkaMessageQueue) OnMessage(msg *connect.KafkaMessage) {
	// // Skip if it came from a wrong topic
	// expectedTopic := c.getTopic()
	// if !strings.Contains(expectedTopic, "*") && expectedTopic != msg.Topic {
	// 	return
	// }

	// Deserialize message
	message, err := c.toMessage(msg)
	if message == nil || err != nil {
		c.Logger.Error("", err, "Failed to read received message")
		return
	}

	c.Counters.IncrementOne("queue." + c.Name() + ".received_messages")
	c.Logger.Debug(message.CorrelationId, "Received message %s via %s", msg, c.Name())

	// Send message to receiver if its set or put it into the queue
	c.Lock.Lock()
	if c.receiver != nil {
		receiver := c.receiver
		c.Lock.Unlock()
		c.sendMessageToReceiver(receiver, message)
	} else {
		c.messages = append(c.messages, *message)
		c.Lock.Unlock()
	}
}

// Clear method are clears component state.
// Parameters:
//   - correlationId 	string (optional) transaction id to trace execution through call chain.
// Returns error or nil no errors occured.
func (c *KafkaMessageQueue) Clear(correlationId string) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	c.messages = make([]cqueues.MessageEnvelope, 0)

	return nil
}

// ReadMessageCount method are reads the current number of messages in the queue to be delivered.
// Returns number of messages or error.
func (c *KafkaMessageQueue) ReadMessageCount() (int64, error) {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	count := (int64)(len(c.messages))
	return count, nil
}

// Peek method are peeks a single incoming message from the queue without removing it.
// If there are no messages available in the queue it returns nil.
// Parameters:
//   - correlationId  string  (optional) transaction id to trace execution through call chain.
// Returns: result *cqueues.MessageEnvelope, err error
// message or error.
func (c *KafkaMessageQueue) Peek(correlationId string) (*cqueues.MessageEnvelope, error) {
	err := c.CheckOpen(correlationId)
	if err != nil {
		return nil, err
	}

	// Subscribe if needed
	err = c.subscribe(correlationId)
	if err != nil {
		return nil, err
	}

	var message *cqueues.MessageEnvelope

	// Pick a message
	c.Lock.Lock()
	if len(c.messages) > 0 {
		message = &c.messages[0]
	}
	c.Lock.Unlock()

	if message != nil {
		c.Logger.Trace(message.CorrelationId, "Peeked message %s on %s", message, c.String())
	}

	return message, nil
}

// PeekBatch method are peeks multiple incoming messages from the queue without removing them.
// If there are no messages available in the queue it returns an empty list.
// Important: This method is not supported by Kafka.
// Parameters:
//   - correlationId     (optional) transaction id to trace execution through call chain.
//   - messageCount      a maximum number of messages to peek.
// Returns:          callback function that receives a list with messages or error.
func (c *KafkaMessageQueue) PeekBatch(correlationId string, messageCount int64) ([]*cqueues.MessageEnvelope, error) {
	err := c.CheckOpen(correlationId)
	if err != nil {
		return nil, err
	}

	// Subscribe if needed
	err = c.subscribe(correlationId)
	if err != nil {
		return nil, err
	}

	c.Lock.Lock()
	batchMessages := c.messages
	if messageCount <= (int64)(len(batchMessages)) {
		batchMessages = batchMessages[0:messageCount]
	}
	c.Lock.Unlock()

	messages := []*cqueues.MessageEnvelope{}
	for _, message := range batchMessages {
		messages = append(messages, &message)
	}

	c.Logger.Trace(correlationId, "Peeked %d messages on %s", len(messages), c.Name())

	return messages, nil
}

// Receive method are receives an incoming message and removes it from the queue.
// Parameters:
//  - correlationId   string   (optional) transaction id to trace execution through call chain.
//  - waitTimeout  time.Duration     a timeout in milliseconds to wait for a message to come.
// Returns:  result *cqueues.MessageEnvelope, err error
// receives a message or error.
func (c *KafkaMessageQueue) Receive(correlationId string, waitTimeout time.Duration) (*cqueues.MessageEnvelope, error) {
	err := c.CheckOpen(correlationId)
	if err != nil {
		return nil, err
	}

	// Subscribe if needed
	err = c.subscribe(correlationId)
	if err != nil {
		return nil, err
	}

	messageReceived := false
	var message *cqueues.MessageEnvelope
	elapsedTime := time.Duration(0)

	for elapsedTime < waitTimeout && !messageReceived {
		c.Lock.Lock()
		if len(c.messages) == 0 {
			c.Lock.Unlock()
			time.Sleep(time.Duration(100) * time.Millisecond)
			elapsedTime += time.Duration(100)
			continue
		}

		// Get message from the queue
		message = &c.messages[0]
		c.messages = c.messages[1:]

		// Add messages to locked messages list
		messageReceived = true
		c.Lock.Unlock()
	}

	return message, nil
}

// Send method are sends a message into the queue.
// Parameters:
//   - correlationId string    (optional) transaction id to trace execution through call chain.
//   - envelope *cqueues.MessageEnvelope  a message envelop to be sent.
// Returns: error or nil for success.
func (c *KafkaMessageQueue) Send(correlationId string, envelop *cqueues.MessageEnvelope) error {
	err := c.CheckOpen(correlationId)
	if err != nil {
		return err
	}

	c.Counters.IncrementOne("queue." + c.Name() + ".sent_messages")
	c.Logger.Debug(envelop.CorrelationId, "Sent message %s via %s", envelop.String(), c.Name())

	msg, err := c.fromMessage(envelop)
	if err != nil {
		return err
	}

	topic := c.Name()
	if topic == "" {
		topic = c.topic
	}

	config := kafka.NewConfig()
	config.Producer.RequiredAcks = kafka.RequiredAcks(c.acks)

	err = c.Connection.Publish(topic, []*kafka.ProducerMessage{msg}, config)
	if err != nil {
		c.Logger.Error(envelop.CorrelationId, err, "Failed to send message via %s", c.Name())
		return err
	}

	return nil
}

// RenewLock method are renews a lock on a message that makes it invisible from other receivers in the queue.
// This method is usually used to extend the message processing time.
// Important: This method is not supported by Kafka.
// Parameters:
//   - message   *cqueues.MessageEnvelope    a message to extend its lock.
//   - lockTimeout  time.Duration  a locking timeout in milliseconds.
// Returns: error
// receives an error or nil for success.
func (c *KafkaMessageQueue) RenewLock(message *cqueues.MessageEnvelope, lockTimeout time.Duration) (err error) {
	// Not supported
	return nil
}

// Complete method are permanently removes a message from the queue.
// This method is usually used to remove the message after successful processing.
// Important: This method is not supported by Kafka.
// Parameters:
//   - message  *cqueues.MessageEnvelope a message to remove.
// Returns: error
// error or nil for success.
func (c *KafkaMessageQueue) Complete(message *cqueues.MessageEnvelope) error {
	err := c.CheckOpen("")
	if err != nil {
		return err
	}

	msg := message.GetReference().(*connect.KafkaMessage)

	// Skip on autocommit
	if c.autoCommit || msg == nil {
		return nil
	}

	// Commit the message offset so it won't come back
	msg.Session.MarkOffset(msg.Message.Topic, msg.Message.Partition, msg.Message.Offset, "")
	msg.Session.Commit()
	message.SetReference(nil)

	return nil
}

// Abandon method are returnes message into the queue and makes it available for all subscribers to receive it again.
// This method is usually used to return a message which could not be processed at the moment
// to repeat the attempt. Messages that cause unrecoverable errors shall be removed permanently
// or/and send to dead letter queue.
// Important: This method is not supported by Kafka.
// Parameters:
//   - message *cqueues.MessageEnvelope  a message to return.
// Returns: error
//  error or nil for success.
func (c *KafkaMessageQueue) Abandon(message *cqueues.MessageEnvelope) error {
	err := c.CheckOpen("")
	if err != nil {
		return err
	}

	msg := message.GetReference().(*connect.KafkaMessage)

	// Skip on autocommit
	if c.autoCommit || msg == nil {
		return nil
	}

	// Seek to the message offset so it will come back
	msg.Session.ResetOffset(msg.Message.Topic, msg.Message.Partition, msg.Message.Offset, "")
	msg.Session.Commit()
	message.SetReference(nil)

	return nil
}

// Permanently removes a message from the queue and sends it to dead letter queue.
// Important: This method is not supported by Kafka.
// Parameters:
//   - message  *cqueues.MessageEnvelope a message to be removed.
// Returns: error
//  error or nil for success.
func (c *KafkaMessageQueue) MoveToDeadLetter(message *cqueues.MessageEnvelope) error {
	// Not supported
	return nil
}

func (c *KafkaMessageQueue) sendMessageToReceiver(receiver cqueues.IMessageReceiver, message *cqueues.MessageEnvelope) {
	correlationId := message.CorrelationId

	defer func() {
		if r := recover(); r != nil {
			err := fmt.Sprintf("%v", r)
			c.Logger.Error(correlationId, nil, "Failed to process the message - "+err)
		}
	}()

	err := receiver.ReceiveMessage(message, c)
	if err != nil {
		c.Logger.Error(correlationId, err, "Failed to process the message")
	}
}

// Listens for incoming messages and blocks the current thread until queue is closed.
// Parameters:
//  - correlationId   string  (optional) transaction id to trace execution through call chain.
//  - receiver    cqueues.IMessageReceiver      a receiver to receive incoming messages.
//
// See IMessageReceiver
// See receive
func (c *KafkaMessageQueue) Listen(correlationId string, receiver cqueues.IMessageReceiver) error {
	err := c.CheckOpen(correlationId)
	if err != nil {
		return err
	}

	// Subscribe if needed
	err = c.subscribe(correlationId)
	if err != nil {
		return err
	}

	c.Logger.Trace("", "Started listening messages at %s", c.Name())

	// Get all collected messages
	c.Lock.Lock()
	batchMessages := c.messages
	c.messages = []cqueues.MessageEnvelope{}
	c.Lock.Unlock()

	// Resend collected messages to receiver
	for _, message := range batchMessages {
		receiver.ReceiveMessage(&message, c)
	}

	// Set the receiver
	c.Lock.Lock()
	c.receiver = receiver
	c.Lock.Unlock()

	return nil
}

// EndListen method are ends listening for incoming messages.
// When this method is call listen unblocks the thread and execution continues.
// Parameters:
//   - correlationId  string   (optional) transaction id to trace execution through call chain.
func (c *KafkaMessageQueue) EndListen(correlationId string) {
	c.Lock.Lock()
	c.receiver = nil
	c.Lock.Unlock()
}
