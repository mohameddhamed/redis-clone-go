package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
)

func Receive(connection net.Conn) string {
	buffer := make([]byte, 1024)
	n, _ := connection.Read(buffer)

	// if err != nil {
	// 	fmt.Println("Error on the Receive fct")
	// 	// os.Exit(0)
	// }

	response := string(buffer[:n])
	return strings.ToLower(response)
}
func parseCommands(cmd string) []string {
	var commands []string

	if len(cmd) > 0 && cmd[0] == '*' {
		arr := strings.Split(cmd[4:], "\r\n")
		for i := 1; i < len(arr); i += 2 {
			commands = append(commands, strings.TrimSpace(arr[i]))
		}
	}
	return commands
}
func contains(arr []string, element string) bool {
	for _, v := range arr {
		if strings.ToLower(v) == element {
			return true
		}
	}
	return false
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

				fmt.Println("[SLAVE] got ok as response from", connection.RemoteAddr())
				fmt.Println("[SLAVE] got ok as response from local", connection.LocalAddr())
				message = arrayType([]string{bulkString("PSYNC"), bulkString("?"), "$2\r\n-1\r\n"}, 3)
				connection.Write([]byte(message))
			}
		}
	}
}
func Propagate(connMap map[string]net.Conn, message string) {

	for _, connection := range connMap {

		if connection == nil {
			fmt.Println("there's no mapped connection")
			return
		}
		// fmt.Println("Remote address:", connection.RemoteAddr())
		// fmt.Println("trying to message", message)

		// connection.Write([]byte(message))
		fmt.Println("[MASTER] this is connection.remoteadd", connection.RemoteAddr())
		fmt.Println("[MASTER] this is connection.localadd", connection.LocalAddr())
		conn, err := net.Dial("tcp", "0.0.0.0:6380")

		if err != nil {
			fmt.Println("Failed to bind to port 6380")
			os.Exit(1)
		}
		fmt.Println("[MASTER] propagating to ", conn.RemoteAddr())

		// message := arrayType([]string{bulkString("PING")}, 1)
		conn.Write([]byte(message))
	}
}

func saveMapToFile(myMap map[string]string, fileName string) {
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Println("error in creating file", fileName)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.Encode(myMap)
}
func retrieveMapFromFile(fileName string) map[string]string {
	myMap := make(map[string]string)
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("there's an error here")
	}
	fmt.Println("opened the file", fileName)
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
func arrayType(arr []string, length int) string {
	message := ""
	for _, s := range arr {
		message += s
	}
	return fmt.Sprintf("*%d\r\n%s", length, message)
}
func RDBFile(content string) string {
	binaryData, err := base64.StdEncoding.DecodeString(content)

	if err != nil {
		fmt.Println("Error on RDBFile fct")
	}
	message := string(binaryData)
	return fmt.Sprintf("$%d\r\n%s", len(message), message)
}
func connect2(port string, host string, role string) {

	listener, err := net.Listen("tcp", host+":"+port)

	if err != nil {
		fmt.Println("Failed to bind to port " + port)
		os.Exit(1)
	}
	fmt.Println("we're listening to", port)

	defer listener.Close()

	// for {
	fmt.Println("I am another ", role)
	fmt.Println("[SLAVE] my network addr is ", listener.Addr())

	connection, err := listener.Accept()

	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		// os.Exit(1)
	}

	fmt.Println("[SLAVE] listening to", connection.RemoteAddr())
	handleConnection(connection, role)
	// }
}
