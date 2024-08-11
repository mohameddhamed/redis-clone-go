package main

import (
	"fmt"
	"strconv"
	"strings"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

func handleConnection(connection net.Conn) {
	for {
		buffer := make([]byte, 256)
		// fmt.Println("before", buffer)
		n, err := connection.Read(buffer)

		// fmt.Println(convert(buffer))
		// fmt.Println("after", buffer)
		// _, resp := ReadNextRESP(buffer[:n])
		// _, resp2 := ReadNextRESP(resp.Data)
		// fmt.Println(string(resp2.Data))
		// fmt.Println(string(buffer))
		// command := string(buffer)
		// fmt.Println(n, string(buffer[:n]))
		var arr []string
		command := string(buffer[:n])
		// fmt.Print(command)
		if len(command) > 0 && command[0] == '*' {
			iterations, _ := strconv.Atoi(string(command[1]))
			i := 2
			iter := 0
			for iter < iterations {
				// if bulk string ahead
				// fmt.Println(string(command[i]))
				if command[i] == '$' {
					i++
					bulkLen, _ := strconv.Atoi(string(command[i]))
					// fmt.Println("bulkLen", bulkLen)
					i += 2
					word := command[i : i+bulkLen+1]
					word = strings.ReplaceAll(word, "\n", "")

					arr = append(arr, word)
					iter++
				}
				i++
			}
		} else {
			arr = append(arr, "1")
			arr = append(arr, "2")
		}
		message := "+PONG\r\n"
		// fmt.Println(arr)
		// fmt.Println(arr[0], arr[1])
		first := strings.ToLower(arr[0])
		if strings.Contains(first, "echo") {
			// message = arr[1]
			message = fmt.Sprintf("$%d\r\n%s\r\n", len(arr[1]), arr[1])
		}

		if err != nil {
			os.Exit(1)
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
	for {

		connection, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(connection)
	}
}
