package raft

import (
	"bytes"
	"encoding/gob"
)

func encodeCommand(command interface{}) ([]byte, error) {
	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(&command); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decodeCommand(data []byte) (interface{}, error) {
	var command interface{}

	err := gob.NewDecoder(
		bytes.NewReader(data),
	).Decode(&command)

	return command, err
}
