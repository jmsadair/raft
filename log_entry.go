package raft

import (
	"encoding/binary"
	"fmt"
	"io"

	pb "github.com/jmsadair/raft/protobuf"
	"google.golang.org/protobuf/proto"
)

type LogEntry struct {
	entry *pb.LogEntry
}

func NewLogEntry(term uint64, index uint64, data []byte) *LogEntry {
	return &LogEntry{entry: &pb.LogEntry{Term: term, Index: index, Data: data}}
}

func (e *LogEntry) Term() uint64 {
	return e.entry.GetTerm()
}

func (e *LogEntry) Index() uint64 {
	return e.entry.GetIndex()
}

func (e *LogEntry) Data() []byte {
	return e.entry.GetData()
}

func (e *LogEntry) Encode(w io.Writer) (int, error) {
	var n int
	var encoded []byte
	var err error

	if encoded, err = proto.Marshal(e.entry); err != nil {
		return 0, fmt.Errorf("error encoding log entry: %s", err.Error())
	}

	// Write length of encoded data that is to follow.
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(encoded)))
	if _, err = w.Write(buf); err != nil {
		return -1, fmt.Errorf("error encoding log entry %s", err.Error())
	}

	// Write encoded data
	if n, err = w.Write(encoded); err != nil {
		return -1, fmt.Errorf("error encoding log entry %s", err.Error())
	}

	return n, nil
}

func (e *LogEntry) Decode(r io.Reader) (int, error) {
	var n int
	var err error

	// Read the length of encoded data that will follow.
	buf := make([]byte, 4)
	if _, err = r.Read(buf); err != nil {
		return -1, fmt.Errorf("error decoding log entry: %s", err.Error())
	}
	length := binary.LittleEndian.Uint32(buf)

	// Read the encoded data.
	encoded := make([]byte, length)
	if n, err = r.Read(encoded); err != nil {
		return -1, fmt.Errorf("error decoding log entry: %s", err.Error())
	}

	if err = proto.Unmarshal(encoded, e.entry); err != nil {
		return -1, fmt.Errorf("error decoding log entry: %s", err.Error())
	}

	return n, err
}
