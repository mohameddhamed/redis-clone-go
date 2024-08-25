package main

import (
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func Receive(connection net.Conn) string {

	buffer := make([]byte, 2048)
	n, _ := connection.Read(buffer)

	// if err != nil {
	// 	fmt.Println("Error on the Receive fct")
	// 	// os.Exit(0)
	// }

	response := string(buffer[:n])
	return strings.ToLower(response)
}

func parseCommands(cmd string) [][]string {
	var allCommands [][]string
	lines := strings.Split(cmd, "\n") // Split input by newlines
	i := 0

	for i < len(lines) {
		line := strings.TrimSpace(lines[i])

		// Skip empty lines
		if line == "" {
			i++
			continue
		}

		// Check for array indicator (e.g., *3 means 3 subsequent items)
		if line[0] == '*' {
			count, err := strconv.Atoi(line[1:])
			if err != nil || count <= 0 {
				i++
				continue // Skip lines that are incorrectly formatted
			}

			var commands []string

			// Process subsequent lines for the specified count
			for j := 0; j < count; j++ {
				i++
				if i >= len(lines) {
					break
				}

				line = strings.TrimSpace(lines[i])
				if strings.HasPrefix(line, "$") {
					// The next line should be the actual command part
					i++
					if i < len(lines) {
						cmdLine := strings.TrimSpace(lines[i])
						commands = append(commands, cmdLine)
					}
				}
			}

			allCommands = append(allCommands, commands)
		} else {
			i++
		}
	}

	return allCommands
}
func contains(arr []string, element string) bool {
	for _, v := range arr {
		if strings.ToLower(v) == element {
			return true
		}
	}
	return false
}

func handshake(masterPort string, host string, slavePort string) net.Conn {

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
	return connection
}
func Propagate(connMap map[string]net.Conn, message string) {

	for _, connection := range connMap {

		if connection == nil {
			fmt.Println("there's no mapped connection")
			return
		}
		connection.Write([]byte(message))
	}
}

// func saveMapToFile(myMap map[string]string, fileName string) {
// 	file, err := os.Create(fileName)
// 	if err != nil {
// 		fmt.Println("error in creating file", fileName)
// 	}
// 	defer file.Close()

// 	encoder := json.NewEncoder(file)
// 	encoder.Encode(myMap)
// }
// func retrieveMapFromFile(fileName string) map[string]string {
// 	myMap := make(map[string]string)
// 	file, err := os.Open(fileName)
// 	if err != nil {
// 		fmt.Println("there's an error here")
// 	}
// 	fmt.Println("opened the file", fileName)
// 	defer file.Close()

// 	content, err := io.ReadAll(file)
// 	if err != nil {
// 		fmt.Println("Error reading file:", err)
// 	}

// 	// Step 3: Print the contents
// 	fmt.Println(string(content))

//		decoder := json.NewDecoder(file)
//		decoder.Decode(&myMap)
//		return myMap
//	}
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

// func connect2(port string, host string, role string) {

// 	listener, err := net.Listen("tcp", host+":"+port)

// 	if err != nil {
// 		fmt.Println("Failed to bind to port " + port)
// 		os.Exit(1)
// 	}
// 	fmt.Println("we're listening to", port)

// 	defer listener.Close()

// 	// for {
// 	fmt.Println("I am another ", role)
// 	fmt.Println("[SLAVE] my network addr is ", listener.Addr())

// 	connection, err := listener.Accept()

// 	if err != nil {
// 		fmt.Println("Error accepting connection: ", err.Error())
// 		// os.Exit(1)
// 	}

// 	fmt.Println("[SLAVE] listening to", connection.RemoteAddr())
// 	handleConnection(connection, role)
// 	// }
// }
