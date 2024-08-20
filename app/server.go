package main

import (
	"flag"
	"fmt"
	"sync"

	// "strconv"
	"strings"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

var mu sync.Mutex
var slavePort string
var connMap = make(map[string]net.Conn) // Map to store connections

func connect(port string, host string, role string) {

	listener, err := net.Listen("tcp", host+":"+port)

	if err != nil {
		fmt.Println("Failed to bind to port " + port)
		os.Exit(1)
	}

	defer listener.Close()

	for {

		connection, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(connection, role)
	}
}

func handleConnection(connection net.Conn, role string) {
	sendFile := false
	layout := "2006-01-02 15:04:05.99999 -0700 MST"
	id := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	emptyRDBContent := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

	defer connection.Close()
	for {

		cmd := Receive(connection)

		commands := parseCommands(cmd)

		message := simpleString("PONG")

		if len(commands) > 0 {

			first := strings.ToLower(commands[0])

			switch {

			case strings.Contains(first, "echo"):

				message = bulkString(commands[1])

			case strings.Contains(first, "set") && contains(commands, "px"):

				message = handleSetPx(commands, layout)

			case strings.Contains(first, "set"):

				message = handleSet(commands)

			case strings.Contains(first, "get"):

				message = handleGet(commands, role, layout)

			case strings.Contains(first, "info"):

				message = handleInfo(commands, role, id)

			case strings.Contains(first, "replconf"):

				message = simpleString("OK")

			case strings.Contains(first, "psync"):

				message = simpleString("FULLRESYNC " + id + " 0")
				sendFile = true

			}
		}

		connection.Write([]byte(message))

		if sendFile {
			connection.Write([]byte(RDBFile(emptyRDBContent)))
			connMap["slave"] = connection
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	host := "0.0.0.0"

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
		handshake(masterPort, masterHost, port)
		role = "slave"
	}
	connect(port, host, role)
}
