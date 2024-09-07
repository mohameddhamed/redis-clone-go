package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"time"
)

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
func sendRDB(conn net.Conn) {

	emptyRDB := []byte("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	buffer := make([]byte, hex.DecodedLen(len(emptyRDB)))
	// TODO: check for errors
	hex.Decode(buffer, emptyRDB)
	conn.Write([]byte(fmt.Sprintf("$%d\r\n", len(buffer))))
	conn.Write(buffer)

}
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
