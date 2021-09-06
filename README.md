# kafkaProducer

## Introduction
kafkaProducer is a simplified kafka producer client. It's designed to be receiving stdin and route to target kafka broker.

## Demo

### Usage
```shell
./kafkaProducer 
  -delay int
    	delay for ms, only work if not in single-message mode (default -1)
  -header value
    	headers
  -host string
    	kafka host
  -print
    	toggle on if want to print the message transferred
  -single-message
    	toggle on if read all the input and send once
  -topic string
    	kafka topic
```

#### Simple case
Receive complete message and send message within single batch with no print out message to stdout
```shell
./kafkaProducer --host {host} --topic {topic}
```

#### Send message and print out sent message
Receive message and send message within single batch and print out message to stdout
```shell
./kafkaProducer --host {host} --topic {topic} --print
```
#### Single line message and send out
Receive message and send message line by line
```shell
./kafkaProducer --host {host} --topic {topic} --single-message
```

#### Single line message and send out with delay for each message 
Receive message and send message line by line
```shell
./kafkaProducer --host {host} --topic {topic} --single-message --delay {x ms}
```
