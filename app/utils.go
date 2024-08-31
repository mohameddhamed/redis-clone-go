package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func handshake(masterConn net.Conn) *bufio.Reader {
	reader := bufio.NewReader(masterConn)
	masterConn.Write([]byte(encodeStringArray([]string{"PING"})))
	reader.ReadString('\n')
	masterConn.Write([]byte(encodeStringArray([]string{"REPLCONF", "listening-port", strconv.Itoa(config.port)})))
	reader.ReadString('\n')
	masterConn.Write([]byte(encodeStringArray([]string{"REPLCONF", "capa", "psync2"})))
	reader.ReadString('\n')
	masterConn.Write([]byte(encodeStringArray([]string{"PSYNC", "?", "-1"})))
	reader.ReadString('\n')
	return reader
}
func receiveRDB(reader *bufio.Reader) {
	response, _ := reader.ReadString('\n')
	if response[0] != '$' {
		fmt.Printf("Invalid response\n")
		os.Exit(1)
	}
	rdbSize, _ := strconv.Atoi(response[1 : len(response)-2])
	buffer := make([]byte, rdbSize)
	receivedSize, err := reader.Read(buffer)
	if err != nil {
		fmt.Printf("Invalid RDB received %v\n", err)
		os.Exit(1)
	}
	if rdbSize != receivedSize {
		fmt.Printf("Size mismatch - got: %d, want: %d\n", receivedSize, rdbSize)
	}

}
func configure() {
	if len(config.replicaofHost) == 0 {

		config.role = "master"
		config.replid = randReplid()

	} else {

		config.role = "slave"
		replicaofArgs := strings.Split(config.replicaofHost, " ")

		switch len(replicaofArgs) {
		case 1:
			config.replicaofPort = 6379
		case 2:
			config.replicaofPort, _ = strconv.Atoi(replicaofArgs[1])
		default:
			flag.Usage()
			os.Exit(1)
		}
		config.replicaofHost = replicaofArgs[0]

	}
}
func connect() {
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.port))
	if err != nil {
		fmt.Printf("Failed to bind to port %d\n", config.port)
		os.Exit(1)
	}
	fmt.Println("Listening on: ", listener.Addr().String())

	store = make(map[string]string)
	ttl = make(map[string]time.Time)

	for id := 1; ; id++ {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go serveClient(id, conn)
	}
}
func randReplid() string {
	chars := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	result := make([]byte, 40)
	for i := range result {
		c := rand.Intn(len(chars))
		result[i] = chars[c]
	}
	return string(result)
}
func serveClient(id int, conn net.Conn) {
	defer conn.Close()
	fmt.Printf("[#%d] Client connected: %v\n", id, conn.RemoteAddr().String())

	for {

		scanner := bufio.NewScanner(conn)
		cmd := []string{}
		var arrSize, strSize int

		for scanner.Scan() {

			token := scanner.Text()
			cmd, arrSize, strSize = parseCommands(token, arrSize, strSize, cmd)

			if arrSize == 0 {
				break
			}
		}

		// TODO: handle scanner errors

		if len(cmd) == 0 {
			break
		}

		fmt.Printf("[#%d] Command = %v\n", id, cmd)
		response, resynch := handleCommand(cmd, 0)

		_, err := conn.Write([]byte(response))

		if err != nil {
			fmt.Printf("[#%d] Error writing response: %v\n", id, err.Error())
			break
		}

		if resynch {
			sendRDB(conn)
			replicas = append(replicas, conn)
		}
	}

	fmt.Printf("[#%d] Client closing\n", id)
}
func sendRDB(conn net.Conn) {

	emptyRDB := []byte("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	buffer := make([]byte, hex.DecodedLen(len(emptyRDB)))
	// TODO: check for errors
	hex.Decode(buffer, emptyRDB)
	conn.Write([]byte(fmt.Sprintf("$%d\r\n", len(buffer))))
	conn.Write(buffer)

}
func parseCommands(token string, arrSize int, strSize int, cmd []string) ([]string, int, int) {

	switch token[0] {
	case '*':
		arrSize, _ = strconv.Atoi(token[1:])
	case '$':
		strSize, _ = strconv.Atoi(token[1:])
	default:
		if len(token) != strSize {
			fmt.Printf("[from master] Wrong string size - got: %d, want: %d\n", len(token), strSize)
			break
		}
		arrSize--
		strSize = 0
		cmd = append(cmd, token)
	}
	return cmd, arrSize, strSize
}
