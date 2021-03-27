# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> Kafka Messaging for Pip.Services in Go Changelog

## <a name="1.1.0"></a> 1.1.0 (2021-03-25)

Migrated to a new messaging model

### Features
* **build** - Added KafkaMessageQueueFactory component
* **connect** - Added KafkaConnection component
* **queues** - Reimplemented the queue to work with shared connection
* **queues** - Added autosubscribe option

## <a name="1.0.1"></a> 1.0.1 (2020-03-13)

### Bug Fixes
Fix blocking code in Listen method

## <a name="1.0.0"></a> 1.0.0 (2020-03-05)

Initial public release

### Features
* **build** factory default implementation
* **connect** components for setting up the connection to the Kafka broker
* **queues** components of working with a message queue