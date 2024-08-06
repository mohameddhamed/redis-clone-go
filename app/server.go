package main

import (
	"fmt"
	// Uncomment this block to pass the first stage
	"net"
	"os"
)

func handleConnection(connection net.Conn) {
	for {
		buffer := make([]byte, 256)
		_, err := connection.Read(buffer)

		if err != nil {
			os.Exit(1)
		}

		message := "+PONG\r\n"
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
