package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

func handleSet(commands []string) string {
	mu.Lock()
	myMap[commands[1]] = commands[2]
	mu.Unlock()

	// Master
	if len(connMap) > 0 {

		propagatedMessage := arrayType([]string{bulkString("SET"), bulkString(commands[1]), bulkString(commands[2])}, 3)
		Propagate(connMap, propagatedMessage)

	}
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
	fmt.Println("this is the map", myMap)

	mu.Lock()
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
func handleSetPx(commands []string, layout string) string {

	currentTime := time.Now()
	expiry, _ := strconv.Atoi(commands[4])
	duration := time.Duration(expiry) * time.Millisecond
	deadline := currentTime.Add(duration).Format(layout)

	value := commands[2] + "|" + deadline
	myMap[commands[1]] = value
	return simpleString("OK")
}
func handlePropagation(connection net.Conn) {
	for {

		cmd := Receive(connection)
		Execute(cmd, "slave")
	}
}
func Execute(cmd string, role string) (string, bool) {
	commands := parseCommands(cmd)

	sendFile := false
	layout := "2006-01-02 15:04:05.99999 -0700 MST"
	id := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	message := simpleString("PONG")

	for _, command := range commands {

		if len(command) > 0 {
			announcement := fmt.Sprintf("[%s] received %s", role, command)
			fmt.Println(announcement)

			first := strings.ToLower(command[0])

			switch {

			case strings.Contains(first, "echo"):

				message = bulkString(command[1])

			case strings.Contains(first, "set") && contains(command, "px"):

				message = handleSetPx(command, layout)

			case strings.Contains(first, "set"):

				// fmt.Println("I received a set cmd")
				// message = handleSet(command)
				if role == "slave" {
					mu.Lock()
					myMap[command[1]] = command[2]
					mu.Unlock()
					fmt.Println("[SLAVE] Updated myMap:", myMap)
					// message = simpleString("OK")
				} else {
					message = handleSet(command)
				}

			case strings.Contains(first, "get"):

				message = handleGet(command, role, layout)

			case strings.Contains(first, "info"):

				message = handleInfo(command, role, id)

			case strings.Contains(first, "replconf"):

				message = simpleString("OK")

			case strings.Contains(first, "psync"):

				message = simpleString("FULLRESYNC " + id + " 0")
				sendFile = true

			}
		}
	}

	return message, sendFile
}
