package main

import (
	"flag"
	"fmt"
	"strconv"
	"sync"
	"time"

	// "strconv"
	"strings"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

var myMap map[string]string
var fileName string
var slaveCount int
var mu sync.Mutex
var slavePort string
var connMap = make(map[string]net.Conn) // Map to store connections

func connect(port string, host string, role string) {

	listener, err := net.Listen("tcp", host+":"+port)

	if err != nil {
		fmt.Println("Failed to bind to port " + port)
		os.Exit(1)
	}
	fmt.Println("we're listening ", port)

	if role == "slave" {
		time.Sleep(2 * time.Second)
	}

	for {
		fmt.Println("I am a ", role)

		connection, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		fmt.Println("listening to", connection.RemoteAddr())

		if role == "slave" {
			go handleAcknowledgment(connection)
		}

		go handleConnection(connection, role)
	}
}

func handleConnection(connection net.Conn, role string) {

	emptyRDBContent := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

	// defer connection.Close()
	for {

		cmd := Receive(connection)

		message, sendFile := Execute(cmd, role)

		connection.Write([]byte(message))

		if sendFile {
			connection.Write([]byte(RDBFile(emptyRDBContent)))

			getAck(connection)

			connMap["slave"+strconv.Itoa(slaveCount)] = connection
			slaveCount++

			sendFile = false
		}

	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	host := "0.0.0.0"
	slaveCount = 0
	fileName = "data.json"

	var port string
	var replicaof string
	role := "master"

	flag.StringVar(&port, "port", "6379", "")
	flag.StringVar(&replicaof, "replicaof", "", "")
	flag.Parse()

	if len(replicaof) > 0 {

		substrings := strings.Split(replicaof, " ")
		masterHost := substrings[0]
		masterPort := substrings[1]
		connection := handshake(masterPort, masterHost, port)
		role = "slave"
		defer connection.Close()

		go handleAcknowledgment(connection)
		go handlePropagation(connection)
	}
	myMap = make(map[string]string)

	connect(port, host, role)
}
