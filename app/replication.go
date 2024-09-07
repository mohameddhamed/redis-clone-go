package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
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

func propagate(cmd []string) {
	if len(replicas) == 0 {
		return
	}
	for i := 0; i < len(replicas); i++ {
		fmt.Printf("Replicating to: %s\n", replicas[i].conn.RemoteAddr().String())
		bytesWritten, err := replicas[i].conn.Write([]byte(encodeStringArray(cmd)))

		if err != nil {
			replicas = removeReplica(replicas, i)
		}

		replicas[i].offset += bytesWritten
	}
}
func removeReplica(replicas []replica, i int) []replica {
	fmt.Printf("Disconnected: %s\n", replicas[i].conn.RemoteAddr().String())
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
