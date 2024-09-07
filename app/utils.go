package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

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

	// store = make(map[string]string)
	// ttl = make(map[string]time.Time)

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
			replicas = append(replicas, replica{conn, 0})
		}
	}

	fmt.Printf("[#%d] Client closing\n", id)
}

func handleWait(count, timeout int) string {
	getAckCmd := []byte(encodeStringArray([]string{"REPLCONF", "GETACK", "*"}))

	acks := 0

	for i := 0; i < len(replicas); i++ {

		if replicas[i].offset > 0 {

			bytesWritten, _ := replicas[i].conn.Write(getAckCmd)
			replicas[i].offset += bytesWritten

			go func(conn net.Conn) {

				fmt.Println("waiting response from replica", conn.RemoteAddr().String())
				buffer := make([]byte, 1024)

				_, err := conn.Read(buffer)
				if err == nil {
					fmt.Println("got response from replica", conn.RemoteAddr().String())
				} else {
					fmt.Println("error from replica", conn.RemoteAddr().String(), " => ", err.Error())
				}

				ackReceived <- true

			}(replicas[i].conn)
		} else {
			acks++
		}
	}

	timer := time.After(time.Duration(timeout) * time.Millisecond)

outer:
	for acks < count {
		select {
		case <-ackReceived:
			acks++
			fmt.Println("acks =", acks)
		case <-timer:
			fmt.Println("timeout! acks =", acks)
			break outer
		}
	}

	return encodeInteger(acks)
}
