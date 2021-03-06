package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/xh-dev-go/xhUtils/xhKafka"
	"io"
	"log"
	"os"
	"time"
)

const VERSION_TAG = "1.0.2"

var topic, server string


var headers xhKafka.KafkaHeaders

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
			Headers: headers.ToKafkaHeaders(),
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

		if hasDelay {
			time.Sleep(delayFor)
		}
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
	var showVersion bool
	flag.StringVar(&topic, "topic", "", "kafka topic")
	flag.StringVar(&server, "host", "", "kafka host")
	flag.BoolVar(&oneOff, "single-message", false, "toggle on if read all the input and send once")
	flag.BoolVar(&shouldPrint, "print", false, "toggle on if want to print the message transferred")
	flag.Int64Var(&delay, "delay", -1, "delay for ms, only work if not in single-message mode")
	flag.Var(&headers, "header", "headers")
	flag.BoolVar(&showVersion, "version", false, "show showVersion")
	flag.Parse()

	if len(os.Args) == 1 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if showVersion {
		println(VERSION_TAG)
		os.Exit(0)
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
