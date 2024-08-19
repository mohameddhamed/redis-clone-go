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

var mu sync.Mutex

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

func handshake(masterPort string, host string, slavePort string) {

	connection, err := net.Dial("tcp", host+":"+masterPort)

	if err != nil {
		fmt.Println("Failed to bind to port " + masterPort)
		os.Exit(1)
	}

	message := arrayType([]string{bulkString("PING")}, 1)
	connection.Write([]byte(message))

	response := Receive(connection)

	if strings.Contains(response, "pong") {

		message = arrayType([]string{bulkString("REPLCONF"), bulkString("listening-port"), bulkString(slavePort)}, 3)
		connection.Write([]byte(message))

		response = Receive(connection)

		if strings.Contains(response, "ok") {

			message = arrayType([]string{bulkString("REPLCONF"), bulkString("capa"), bulkString("psync2")}, 3)
			connection.Write([]byte(message))

			response = Receive(connection)

			if strings.Contains(response, "ok") {

				message = arrayType([]string{bulkString("PSYNC"), bulkString("?"), "$2\r\n-1\r\n"}, 3)
				connection.Write([]byte(message))
			}
		}
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
		var commands []string

		if len(cmd) > 0 && cmd[0] == '*' {
			arr := strings.Split(cmd[4:], "\r\n")
			for i := 1; i < len(arr); i += 2 {
				commands = append(commands, strings.TrimSpace(arr[i]))
			}
		}

		message := simpleString("PONG")

		if len(commands) > 0 {

			first := strings.ToLower(commands[0])

			if strings.Contains(first, "echo") {
				message = bulkString(commands[1])

			} else if strings.Contains(first, "set") && contains(commands, "px") {
				myMap := make(map[string]string)

				currentTime := time.Now()
				expiry, _ := strconv.Atoi(commands[4])
				duration := time.Duration(expiry) * time.Millisecond
				deadline := currentTime.Add(duration).Format(layout)

				value := commands[2] + "|" + deadline
				myMap[commands[1]] = value
				saveMapToFile(myMap)
				message = simpleString("OK")

			} else if strings.Contains(first, "set") {
				myMap := make(map[string]string)
				myMap[commands[1]] = commands[2]
				saveMapToFile(myMap)
				message = simpleString("OK")

			} else if strings.Contains(first, "get") {
				key := commands[1]

				mu.Lock()
				myMap := retrieveMapFromFile()
				value := myMap[key]
				mu.Unlock()

				if !strings.Contains(value, "|") {
					message = bulkString(value)
				} else {
					args := strings.Split(value, "|")
					deadline, _ := time.Parse(layout, args[1])

					if deadline.After(time.Now()) {
						message = bulkString((args[0]))
					} else {
						message = bulkString("-1")
					}
				}
			} else if strings.Contains(first, "info") {

				key := strings.ToLower(commands[1])

				if strings.Contains(key, "replication") {

					messageBefore := "role:" + role + "\n"
					messageBefore += "master_replid:" + id + "\n"
					offset := 0
					messageBefore += "master_repl_offset:" + strconv.Itoa(offset)

					message = bulkString(messageBefore)

				}
			} else if strings.Contains(first, "replconf") {

				message = simpleString("OK")

			} else if strings.Contains(first, "psync") {
				message = simpleString("FULLRESYNC " + id + " 0")
				sendFile = true
			}
		}

		connection.Write([]byte(message))
		if sendFile {
			connection.Write([]byte(RDBFile(emptyRDBContent)))
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
