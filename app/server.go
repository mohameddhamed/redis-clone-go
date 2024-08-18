package main

import (
	"encoding/json"
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

func handshake(port string, host string) {

	connection, err := net.Dial("tcp", host+":"+port)

	if err != nil {
		fmt.Println("Failed to bind to port " + port)
		os.Exit(1)
	}

	message := arrayType(bulkString("PING"), 1)
	connection.Write([]byte(message))
}

func contains(arr []string, element string) bool {
	for _, v := range arr {
		if strings.ToLower(v) == element {
			return true
		}
	}
	return false
}

func saveMapToFile(myMap map[string]string) {
	file, _ := os.Create("data.json")
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.Encode(myMap)
}
func retrieveMapFromFile() map[string]string {
	myMap := make(map[string]string)
	file, _ := os.Open("data.json")
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.Decode(&myMap)
	return myMap
}
func simpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}
func bulkString(s string) string {
	if s == "-1" {
		return fmt.Sprintf("$%s\r\n", s)
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}
func arrayType(s string, length int) string {
	return fmt.Sprintf("*%d\r\n%s", length, s)
}

func handleConnection(connection net.Conn, role string) {
	layout := "2006-01-02 15:04:05.99999 -0700 MST"

	defer connection.Close()
	for {
		buffer := make([]byte, 1024)
		n, err := connection.Read(buffer)

		if err != nil {
			return
		}

		var commands []string
		cmd := string(buffer[:n])

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
					id := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
					messageBefore += "master_replid:" + id + "\n"
					offset := 0
					messageBefore += "master_repl_offset:" + strconv.Itoa(offset)

					message = bulkString(messageBefore)

				}
			}
		}

		connection.Write([]byte(message))
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
		handshake(masterPort, masterHost)
		role = "slave"
		// connect(port, host, "slave")
	}
	// } else {
	connect(port, host, role)
}
