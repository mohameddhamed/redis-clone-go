package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

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
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func handleConnection(connection net.Conn) {
	defer connection.Close()
	for {
		buffer := make([]byte, 256)
		n, err := connection.Read(buffer)

		var arr []string
		command := string(buffer[:n])
		if len(command) > 0 && command[0] == '*' {
			iterations, _ := strconv.Atoi(string(command[1]))
			i := 2
			iter := 0
			for iter < iterations {
				// if bulk string ahead
				if command[i] == '$' {
					i++
					bulkLen, _ := strconv.Atoi(string(command[i]))
					i += 2
					word := command[i : i+bulkLen+1]
					word = strings.ReplaceAll(word, "\n", "")

					arr = append(arr, word)
					iter++
				}
				i++
			}
		}

		message := simpleString("PONG")

		if len(arr) > 0 {

			first := strings.ToLower(arr[0])

			if strings.Contains(first, "echo") {
				message = bulkString(arr[1])

			} else if strings.Contains(first, "set") {
				myMap := make(map[string]string)
				myMap[arr[1]] = arr[2]
				saveMapToFile(myMap)
				message = simpleString("OK")

			} else if strings.Contains(first, "get") {
				key := arr[1]
				myMap := retrieveMapFromFile()
				message = bulkString(myMap[key])
			}
		}

		if err != nil {
			os.Exit(0)
		}

		connection.Write([]byte(message))
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
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
