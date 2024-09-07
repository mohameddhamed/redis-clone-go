package main

import (
	"fmt"
	"strconv"
)

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
