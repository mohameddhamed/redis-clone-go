package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func Receive(connection net.Conn) string {
	buffer := make([]byte, 1024)
	n, err := connection.Read(buffer)

	if err != nil {
		fmt.Println("Error on the Receive fct")
		// os.Exit(0)
	}

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
func handleSet(commands []string) string {
	myMap := make(map[string]string)
	myMap[commands[1]] = commands[2]

	propagatedMessage := arrayType([]string{bulkString("SET"), bulkString(commands[1]), bulkString(commands[2])}, 3)
	Propagate(connMap, propagatedMessage)

	saveMapToFile(myMap, "data.json")
	return simpleString("OK")
}
func handleInfo(commands []string, role string, id string) string {
	message := ""
	key := strings.ToLower(commands[1])

	if strings.Contains(key, "replication") {

		messageBefore := "role:" + role + "\n"
		messageBefore += "master_replid:" + id + "\n"
		offset := 0
		messageBefore += "master_repl_offset:" + strconv.Itoa(offset)

		message = bulkString(messageBefore)

	}
	return message
}
func handleGet(commands []string, role string, layout string) string {
	message := ""
	key := commands[1]
	fileName := "data.json"

	if role == "slave" {
		fileName = "data-replica.json"
	}

	mu.Lock()
	myMap := retrieveMapFromFile(fileName)
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
	return message
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

				message = arrayType([]string{bulkString("PSYNC"), bulkString("?"), "$2\r\n-1\r\n"}, 3)
				connection.Write([]byte(message))
			}
		}
	}
}
func Propagate(connMap map[string]net.Conn, message string) {

	fmt.Println("propagating", slavePort)
	connection := connMap["slave"]

	if connection == nil {
		fmt.Println("there's no mapped connection")
		return
	}

	connection.Write([]byte(message))
}
func handleSetPx(commands []string, layout string) string {
	myMap := make(map[string]string)

	currentTime := time.Now()
	expiry, _ := strconv.Atoi(commands[4])
	duration := time.Duration(expiry) * time.Millisecond
	deadline := currentTime.Add(duration).Format(layout)

	value := commands[2] + "|" + deadline
	myMap[commands[1]] = value
	saveMapToFile(myMap, "data.json")
	return simpleString("OK")
}

func saveMapToFile(myMap map[string]string, fileName string) {
	file, _ := os.Create(fileName)
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.Encode(myMap)
}
func retrieveMapFromFile(fileName string) map[string]string {
	myMap := make(map[string]string)
	file, _ := os.Open(fileName)
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
