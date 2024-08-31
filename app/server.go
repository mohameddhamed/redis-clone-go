package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type serverConfig struct {
	port          int
	role          string
	replid        string
	replOffset    int
	replicaofHost string
	replicaofPort int
}

var store map[string]string
var ttl map[string]time.Time
var config serverConfig
var replicas []net.Conn

func main() {

	flag.IntVar(&config.port, "port", 6379, "listen on specified port")
	flag.StringVar(&config.replicaofHost, "replicaof", "", "start server in replica mode of given host and port")
	flag.Parse()

	configure()

	if config.role == "slave" {

		masterConn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", config.replicaofHost, config.replicaofPort))

		if err != nil {
			fmt.Printf("Failed to connect to master %v\n", err)
			os.Exit(1)
		}
		defer masterConn.Close()

		reader := handshake(masterConn)

		receiveRDB(reader)
		totalProcessedBytes := 0

		go handlePropagation(reader, masterConn, totalProcessedBytes)
	}

	connect()
}

func encodeBulkString(s string) string {
	if len(s) == 0 {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}
func encodeInteger(number int) string {
	return fmt.Sprintf(":%d\r\n", number)
}

func encodeStringArray(arr []string) string {
	result := fmt.Sprintf("*%d\r\n", len(arr))
	for _, s := range arr {
		result += encodeBulkString(s)
	}
	return result
}

func handleCommand(cmd []string, byteCount int) (response string, resynch bool) {
	isWrite := false
	switch strings.ToUpper(cmd[0]) {
	case "COMMAND":
		response = "+OK\r\n"
	case "REPLCONF":
		if len(cmd) >= 2 {
			if strings.ToUpper(cmd[1]) == "GETACK" {
				response = encodeStringArray([]string{"REPLCONF", "ACK", strconv.Itoa(byteCount)})
			} else {
				// TODO: Implement proper replication
				response = "+OK\r\n"
			}
		}
	case "PSYNC":
		if len(cmd) == 3 {
			// TODO: Implement synch
			response = fmt.Sprintf("+FULLRESYNC %s 0\r\n", config.replid)
			resynch = true
		}
	case "PING":
		response = "+PONG\r\n"
	case "ECHO":
		response = encodeBulkString(cmd[1])
	case "INFO":
		if len(cmd) == 2 && strings.ToUpper(cmd[1]) == "REPLICATION" {
			response = encodeBulkString(fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d",
				config.role, config.replid, config.replOffset))
		}
	case "SET":
		isWrite = true
		// TODO: check length
		key, value := cmd[1], cmd[2]
		store[key] = value

		if len(cmd) == 5 && strings.ToUpper(cmd[3]) == "PX" {
			expiration, _ := strconv.Atoi(cmd[4])
			ttl[key] = time.Now().Add(time.Millisecond * time.Duration(expiration))
		}
		response = "+OK\r\n"

	case "GET":
		// TODO: check length
		key := cmd[1]
		value, ok := store[key]

		if ok {
			expiration, exists := ttl[key]
			if !exists || expiration.After(time.Now()) {
				response = encodeBulkString(value)
			} else if exists {
				delete(ttl, key)
				delete(store, key)
				response = encodeBulkString("")
			}

		} else {
			response = encodeBulkString("")
		}
	case "WAIT":
		response = encodeInteger(0)
	}
	if isWrite {
		propagate(cmd)
	}
	return
}

func propagate(cmd []string) {
	if len(replicas) == 0 {
		return
	}
	for i := 0; i < len(replicas); i++ {
		fmt.Printf("Replicating to: %s\n", replicas[i].RemoteAddr().String())
		_, err := replicas[i].Write([]byte(encodeStringArray(cmd)))

		if err != nil {
			replicas = removeReplica(replicas, i)
		}
	}
}
func removeReplica(replicas []net.Conn, i int) []net.Conn {
	fmt.Printf("Disconnected: %s\n", replicas[i].RemoteAddr().String())
	if len(replicas) > 1 {
		last := len(replicas) - 1
		replicas[i] = replicas[last]
		replicas = replicas[:last]
		i--
	}
	return replicas
}

func handlePropagation(reader *bufio.Reader, masterConn net.Conn, totalProcessedBytes int) {
	for {
		cmd := []string{}
		var arrSize, strSize int
		byteCount := 0
		for {
			token, err := reader.ReadString('\n')
			byteCount += len(token)

			if err != nil {
				return
			}
			token = strings.TrimRight(token, "\r\n")

			cmd, arrSize, strSize = parseCommands(token, arrSize, strSize, cmd)

			if arrSize == 0 {
				break
			}

		}

		// TODO: handle scanner errors

		if len(cmd) == 0 {
			break
		}

		fmt.Printf("[from master] Command = %q\n", cmd)
		response, _ := handleCommand(cmd, totalProcessedBytes)
		totalProcessedBytes += byteCount
		fmt.Printf("response = %q\n", response)

		if strings.ToUpper(cmd[0]) == "REPLCONF" {

			fmt.Printf("ack = %q\n", cmd)
			_, err := masterConn.Write([]byte(response))

			if err != nil {
				fmt.Printf("Error responding to master: %v\n", err.Error())
				break
			}
		}
	}
}
