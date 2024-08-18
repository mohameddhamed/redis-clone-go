package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
)

func Receive(connection net.Conn) string {
	buffer := make([]byte, 1024)
	n, err := connection.Read(buffer)

	if err != nil {
		fmt.Println(err.Error())
	}

	response := string(buffer[:n])
	return strings.ToLower(response)
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
func arrayType(arr []string, length int) string {
	message := ""
	for _, s := range arr {
		message += s
	}
	return fmt.Sprintf("*%d\r\n%s", length, message)
}
