package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type replica struct {
	conn   net.Conn
	offset int
}

type serverConfig struct {
	port          int
	role          string
	replid        string
	replOffset    int
	replicaofHost string
	replicaofPort int
	dir           string
	dbFileName    string
}

var ackReceived chan bool

// var mu sync.Mutex
var store map[string]string
var ttl map[string]time.Time
var config serverConfig
var replicas []replica

// var numAcknowledgedReplicas int
// const (
// 	opCodeAuxField   byte = 0xFA // key, value follow
// 	opCodeSelectDB   byte = 0xFE // following byte is db number
// 	opCodeResizeDB   byte = 0xFB // follwing are 2 length-encoded ints
// 	opCodeTypeString byte = 0x00 // following byte(s) are length encoding
// 	opCodeEOF        byte = 0xFF // following 8 bytes are CRC64 checksum
// )

func main() {

	// numAcknowledgedReplicas = 0

	flag.IntVar(&config.port, "port", 6379, "listen on specified port")
	flag.StringVar(&config.replicaofHost, "replicaof", "", "start server in replica mode of given host and port")

	flag.StringVar(&config.dir, "dir", "", "the path to the directory where the RDB file is stored")
	flag.StringVar(&config.dbFileName, "dbfilename", "", "the name of the RDB file ")

	flag.Parse()

	store = make(map[string]string)
	ttl = make(map[string]time.Time)
	ackReceived = make(chan bool)

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

	if len(config.dir) > 0 && len(config.dbFileName) > 0 {
		rdbPath := filepath.Join(config.dir, config.dbFileName)
		err := readRDB(rdbPath)
		if err != nil {
			fmt.Printf("Failed to load '%s': %v\n", rdbPath, err)
		}
	}

	connect()
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

			} else if config.role == "master" && strings.ToUpper(cmd[1]) == "ACK" {
				// mu.Lock()
				// numAcknowledgedReplicas++
				// mu.Unlock()
				ackReceived <- true
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
	case "INCR":
		// TODO: check length
		key := cmd[1]
		value, ok := store[key]
		if ok {
			numValue, _ := strconv.Atoi(value)
			numValue++
			store[key] = strconv.Itoa(numValue)

			response = encodeInteger(numValue)
		} else {
			store[key] = "1"
			response = encodeInteger(1)
		}
	case "WAIT":
		// response = encodeInteger(len(replicas))
		numReplicas, _ := strconv.Atoi(cmd[1])
		timeout, _ := strconv.Atoi(cmd[2])
		response = handleWait(numReplicas, timeout)
		// numAcknowledgedReplicas = 0
	case "KEYS":
		keys := make([]string, 0, len(store))
		for key := range store {
			keys = append(keys, key)
		}
		response = encodeStringArray(keys)

	case "CONFIG":
		if strings.ToLower(cmd[1]) == "get" {
			switch strings.ToLower(cmd[2]) {
			case "dir":
				response = encodeStringArray([]string{"dir", config.dir})
			case "dbfilename":
				response = encodeStringArray([]string{"dbfilename", config.dbFileName})
			}
		}
	}
	if isWrite {
		propagate(cmd)
	}
	return
}
