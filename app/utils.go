package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"slices"
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

// func sliceIndex(data []byte, sep byte) int {
// 	for i, b := range data {
// 		if b == sep {
// 			return i
// 		}
// 	}
// 	return -1
// }
// func parseTable(bytes []byte) []byte {
// 	start := sliceIndex(bytes, opCodeResizeDB)
// 	end := sliceIndex(bytes, opCodeEOF)
// 	return bytes[start+1 : end]
// }

//	func handleKeys(path string, pattern string) string {
//		c, _ := os.Open(path)
//		res, _ := ParseRDB(c)
//		if len(res) == 0 {
//			return ""
//		}
//		for i := 0; i < len(res); i += 2 {
//			KeyValueStore[res[i]] = KeyVal{
//				value:        res[i+1],
//				expiryTime:   -1,
//				insertedTime: time.Now(),
//			}
//		}
//		return string(res[0])
//	}
//
//	func ParseRDB(file *os.File) ([]string, error) {
//		reader := bufio.NewReader(file)
//		result := []string{}
//		// Read header.
//		header := make([]byte, 9)
//		_, err := reader.Read(header)
//		if err != nil {
//			return result, err
//		}
//		// Skip the junk after the header.
//		if _, err := reader.ReadBytes(opCodeResizeDB); err != nil {
//			return result, err
//		}
//		if _, err := reader.Read(make([]byte, 2)); err != nil {
//			return result, err
//		}
//		for {
//			opcode, err := reader.ReadByte()
//			fmt.Println(opcode)
//			if err != nil {
//				return result, err
//			}
//			switch opcode {
//			case opCodeSelectDB:
//				// Follwing byte(s) is the db number.
//				_, err := decodeLength(reader)
//				if err != nil {
//					return result, err
//				}
//			case opCodeAuxField:
//				// Length prefixed key and value strings follow.
//				kv := [][]byte{}
//				for i := 0; i < 2; i++ {
//					length, err := decodeLength(reader)
//					if err != nil {
//						return result, err
//					}
//					data := make([]byte, int(length))
//					if _, err = reader.Read(data); err != nil {
//						return result, err
//					}
//					kv = append(kv, data)
//				}
//			case opCodeResizeDB:
//				// Implement
//			case opCodeTypeString:
//				kv := [][]byte{}
//				for i := 0; i < 2; i++ {
//					length, err := decodeLength(reader)
//					if err != nil {
//						return result, err
//					}
//					data := make([]byte, int(length))
//					if _, err = reader.Read(data); err != nil {
//						return result, err
//					}
//					kv = append(kv, data)
//				}
//				result = append(result, string(kv[0]), string(kv[1]))
//			case opCodeEOF:
//				// Get the 8-byte checksum after this
//				checksum := make([]byte, 8)
//				_, err := reader.Read(checksum)
//				if err != nil {
//					return result, err
//				}
//				return result, nil
//			default:
//				// Handle any other tags.
//			}
//		}
//	}
//
//	func decodeLength(r *bufio.Reader) (int, error) {
//		num, err := r.ReadByte()
//		if err != nil {
//			return 0, err
//		}
//		switch {
//		case num <= 63: // leading bits 00
//			// Remaining 6 bits are the length.
//			return int(num & 0b00111111), nil
//		case num <= 127: // leading bits 01
//			// Remaining 6 bits plus next byte are the length
//			nextNum, err := r.ReadByte()
//			if err != nil {
//				return 0, err
//			}
//			length := binary.BigEndian.Uint16([]byte{num & 0b00111111, nextNum})
//			return int(length), nil
//		case num <= 191: // leading bits 10
//			// Next 4 bytes are the length
//			bytes := make([]byte, 4)
//			_, err := r.Read(bytes)
//			if err != nil {
//				return 0, err
//			}
//			length := binary.BigEndian.Uint32(bytes)
//			return int(length), nil
//		case num <= 255: // leading bits 11
//			// Next 6 bits indicate the format of the encoded object.
//			// TODO: This will result in problems on the next read, possibly.
//			valueType := num & 0b00111111
//			return int(valueType), nil
//		default:
//			return 0, err
//		}
//	}
func readEncodedInt(reader *bufio.Reader) (int, error) {
	mask := byte(0b11000000)
	b0, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}
	if b0&mask == 0b00000000 {
		return int(b0), nil
	} else if b0&mask == 0b01000000 {
		b1, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return int(b1)<<6 | int(b0&mask), nil
	} else if b0&mask == 0b10000000 {
		b1, _ := reader.ReadByte()
		b2, _ := reader.ReadByte()
		b3, _ := reader.ReadByte()
		b4, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		// TODO: check endianness!
		return int(b1)<<24 | int(b2)<<16 | int(b3)<<8 | int(b4), nil
	} else if b0 >= 0b11000000 && b0 <= 0b11000010 { // Special format: Integers as String
		var b1, b2, b3, b4 byte
		b1, err = reader.ReadByte()
		if b0 >= 0b11000001 {
			b2, err = reader.ReadByte()
		}
		if b0 == 0b11000010 {
			b3, _ = reader.ReadByte()
			b4, err = reader.ReadByte()
		}
		if err != nil {
			return 0, err
		}
		return int(b1) | int(b2)<<8 | int(b3)<<16 | int(b4)<<24, nil
	} else {
		return 0, errors.New("not implemented")
	}
}

func readEncodedString(reader *bufio.Reader) (string, error) {
	size, err := readEncodedInt(reader)
	if err != nil {
		return "", err
	}
	data := make([]byte, size)
	actual, err := reader.Read(data)
	if err != nil {
		return "", err
	}
	if int(size) != actual {
		return "", errors.New("unexpected string length")
	}
	return string(data), nil
}

func readRDB(rdbPath string) error {
	file, err := os.Open(rdbPath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	header := make([]byte, 9)
	reader.Read(header)
	if slices.Compare(header[:5], []byte("REDIS")) != 0 {
		return errors.New("not a RDB file")
	}

	version, _ := strconv.Atoi(string(header[5:]))
	fmt.Printf("File version: %d\n", version)

	for eof := false; !eof; {

		startDataRead := false
		opCode, err := reader.ReadByte()

		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// TODO: handle errors properly
		switch opCode {
		case 0xFA: // Auxiliary fields
			key, _ := readEncodedString(reader)
			switch key {
			case "redis-ver":
				value, _ := readEncodedString(reader)
				fmt.Printf("Aux: %s = %v\n", key, value)
			case "redis-bits":
				bits, _ := readEncodedInt(reader)
				fmt.Printf("Aux: %s = %v\n", key, bits)
			case "ctime":
				ctime, _ := readEncodedInt(reader)
				fmt.Printf("Aux: %s = %v\n", key, ctime)
			case "used-mem":
				usedmem, _ := readEncodedInt(reader)
				fmt.Printf("Aux: %s = %v\n", key, usedmem)
			case "aof-preamble":
				size, _ := readEncodedInt(reader)
				// preamble := make([]byte, size)
				// reader.Read(preamble)
				fmt.Printf("Aux: %s = %d\n", key, size)
			default:
				fmt.Printf("Unknown auxiliary field: %q\n", key)
			}

		case 0xFB: // Hash table sizes for the main keyspace and expires
			keyspace, _ := readEncodedInt(reader)
			expires, _ := readEncodedInt(reader)
			fmt.Printf("Hash table sizes: keyspace = %d, expires = %d\n", keyspace, expires)
			startDataRead = true

		case 0xFE: // Database Selector
			db, _ := readEncodedInt(reader)
			fmt.Printf("Database Selector = %d\n", db)

		case 0xFF: // End of the RDB file
			eof = true
		default:
			fmt.Printf("Unknown op code: %d\n", opCode)
		}

		if startDataRead {
			for {
				valueType, err := reader.ReadByte()
				if err != nil {
					return err
				}

				// TODO: handle expiry
				var expiration time.Time
				if valueType == 0xFD {
					bytes := make([]byte, 4)
					reader.Read(bytes)
					expiration = time.Unix(int64(bytes[0])|int64(bytes[1])<<8|int64(bytes[2])<<16|int64(bytes[3])<<24, 0)
					valueType, err = reader.ReadByte()
				} else if valueType == 0xFC {
					bytes := make([]byte, 8)
					reader.Read(bytes)
					expiration = time.UnixMilli(int64(bytes[0]) | int64(bytes[1])<<8 | int64(bytes[2])<<16 | int64(bytes[3])<<24 |
						int64(bytes[4])<<32 | int64(bytes[5])<<40 | int64(bytes[6])<<48 | int64(bytes[7])<<56)
					valueType, err = reader.ReadByte()
				}

				if err != nil {
					return err
				}

				if valueType > 14 {
					startDataRead = false
					reader.UnreadByte()
					break
				}

				key, _ := readEncodedString(reader)
				value, _ := readEncodedString(reader)
				fmt.Printf("Reading key/value: %q => %q Expiration: (%v)\n", key, value, expiration)

				now := time.Now()

				if expiration.IsZero() || expiration.After(now) {
					if expiration.After(now) {
						ttl[key] = expiration
					}
					store[key] = value
				}
			}
		}
	}

	return nil
}
func readEncodedLong(reader *bufio.Reader) (uint64, error) {
	var value uint64
	err := binary.Read(reader, binary.LittleEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}
func readUint32LittleEndian(reader *bufio.Reader) (uint32, error) {
	var value uint32
	err := binary.Read(reader, binary.LittleEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
	// bytes := make([]byte, 4)
	// _, err := reader.Read(bytes)
	// if err != nil {
	// 	return 0, err
	// }

	// // Little-endian means we start from the least significant byte
	// return uint32(bytes[0]) | uint32(bytes[1])<<8 | uint32(bytes[2])<<16 | uint32(bytes[3])<<24, nil
}

func readBytes(reader io.Reader, numBytes uint64) ([]byte, error) {
	buf := make([]byte, numBytes)
	n, err := io.ReadFull(reader, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
func readByte(reader io.Reader) (byte, error) {
	buf := make([]byte, 1)
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}
