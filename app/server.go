package main

import (
	"encoding/json"
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

func handleConnection(connection net.Conn) {
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
			}
		}

		connection.Write([]byte(message))
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	port := "6379"

	if len(os.Args) > 1 {
		if os.Args[1] == "--port" {
			port = os.Args[2]
		}
	}

	// Uncomment this block to pass the first stage
	//
	listener, err := net.Listen("tcp", "0.0.0.0:"+port)
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

		go handleConnection(connection)
	}
}
