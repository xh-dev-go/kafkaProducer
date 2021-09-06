package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/segmentio/kafka-go"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

var topic, server string

type CustHeaders []kafka.Header

func (i *CustHeaders) toKafkaHeaders() []kafka.Header {
	return *i
}

func (i *CustHeaders) String() string {
	var str = ""

	for _, item := range *i {
		str += item.Key
		str += "="
		str += string(item.Value)
		str += ", "
	}
	if len(str) > 0 {
		return str[:len(str)-2]
	} else {
		return str
	}
}
func (i *CustHeaders) Set(value string) error {
	vs := strings.Split(value, "=")
	if len(vs) != 2 {
		panic("Header not with correct format: " + value)
	} else {
		*i = append(*i, kafka.Header{
			Key:   vs[0],
			Value: []byte(vs[1]),
		})
	}
	return nil
}

var headers CustHeaders

func kafkaProducer(server string) *kafka.Writer {
	w := &kafka.Writer{
		Addr: kafka.TCP(server),
		// NOTE: When Topic is not defined here, each Message must define it instead.
		Balancer: &kafka.LeastBytes{},
	}
	return w
}
func send_message(writer *kafka.Writer, topic string, key string, msg string) {

	err := writer.WriteMessages(context.Background(),
		// NOTE: Each Message has Topic defined, otherwise an error is returned.
		kafka.Message{
			Topic:   topic,
			Key:     []byte(key),
			Value:   []byte(msg),
			Headers: headers.toKafkaHeaders(),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
}

func readInput(writer *kafka.Writer, topic string, doneChan chan bool, delay int64, shouldPrint bool) {
	reader := bufio.NewReader(os.Stdin)
	var index = 0

	var delayFor time.Duration
	var hasDelay = false
	if delay > 0 {
		hasDelay = true
		delayFor = time.Duration(delay) * time.Millisecond
	}

	for {
		if hasDelay {
			time.Sleep(delayFor)
		}
		index++
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			doneChan <- true
			break
		}

		if shouldPrint {
			println(string(line))
		}
		send_message(writer, topic, fmt.Sprintf("N_key_%s", time.Now().Format(time.RFC3339)), string(line))
		//println(line)
	}
}

func readAllInput(writer *kafka.Writer, topic string, doneChan chan bool, shouldPrint bool) {
	reader := bufio.NewReader(os.Stdin)
	var res = ""
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		res = res + "\n" + string(line)
	}
	if shouldPrint {
		println(res)
	}
	send_message(writer, topic, fmt.Sprintf("N_key_%s", time.Now().Format(time.RFC3339)), res)
	doneChan <- true
}

func main() {
	var oneOff bool
	var shouldPrint bool
	var delay int64
	flag.StringVar(&topic, "topic", "", "kafka topic")
	flag.StringVar(&server, "host", "", "kafka host")
	flag.BoolVar(&oneOff, "single-message", false, "toggle on if read all the input and send once")
	flag.BoolVar(&shouldPrint, "print", false, "toggle on if want to print the message transferred")
	flag.Int64Var(&delay, "delay", -1, "delay for ms, only work if not in single-message mode")
	flag.Var(&headers, "header", "headers")
	flag.Parse()

	if len(os.Args) == 1 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if topic == "" {
		panic(errors.New("topic is empty"))
	}
	if server == "" {
		panic(errors.New("server is empty"))
	}

	writer := kafkaProducer(server)

	var doneChan = make(chan bool)
	if oneOff {
		go readAllInput(writer, topic, doneChan, shouldPrint)
	} else {
		go readInput(writer, topic, doneChan, delay, shouldPrint)
	}

	for {
		select {
		case isDone := <-doneChan:
			if isDone {
				os.Exit(0)
			}
		}
	}
}
