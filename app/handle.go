package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func handleSet(commands []string) string {
	myMap := make(map[string]string)
	myMap[commands[1]] = commands[2]

	suffix := ""
	fmt.Println("this is the len", len(connMap))

	// Master
	if len(connMap) > 0 {
		fmt.Println("I am the master")

		propagatedMessage := arrayType([]string{bulkString("SET"), bulkString(commands[1]), bulkString(commands[2])}, 3)
		Propagate(connMap, propagatedMessage)

	} else {
		fmt.Println("oh my")
		// Replica
		// currentTime := time.Now().Format("15:04:05")
		// suffix = "replica" + currentTime
	}
	fileName = "data" + suffix + ".json"
	fmt.Println("the filename is ", fileName)
	saveMapToFile(myMap, fileName)
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
	// fileName := "data.json"

	// if role == "slave" {
	// 	fileName = "data-replica.json"
	// }

	mu.Lock()
	myMap := retrieveMapFromFile(fileName)
	value := myMap[key]
	mu.Unlock()

	fmt.Printf(role)
	// value = "456"

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
