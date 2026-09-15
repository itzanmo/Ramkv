package main

import (
	"encoding/binary"
	"io"
	"log"
	"net"
)

const (
	maxKeyLen = 4096
	maxValLen = 16 * 1024 * 1024
)

type Entry struct {
	key string
	val []byte
}

type Response struct {
	ok   bool
	data []byte
}

type Payload struct {
	wayback chan Response
	entry   Entry
}

type ListEntry struct {
	pos    int
	length int
}

func main() {
	datapipe := make(chan Payload, 50)
	ledger := make([]byte, 0, 1024*1024)
	list := make(map[string]ListEntry)
	go Processor(datapipe, &ledger, list)

	l, err := net.Listen("tcp", ":5075")
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		go Worker(conn, datapipe)
	}
}

func Worker(conn net.Conn, pipe chan Payload) {
	defer conn.Close()
	action := make([]byte, 1)

	for {
		if _, err := io.ReadFull(conn, action); err != nil {
			return
		}

		switch action[0] {
		case 0x01:
			if _, err := conn.Write([]byte{0x01}); err != nil {
				return
			}

		case 0x02:
			keyBuf := make([]byte, 2)
			if _, err := io.ReadFull(conn, keyBuf); err != nil {
				return
			}
			keyLen := binary.BigEndian.Uint16(keyBuf)
			if keyLen == 0 || int(keyLen) > maxKeyLen {
				return
			}

			key := make([]byte, keyLen)
			if _, err := io.ReadFull(conn, key); err != nil {
				return
			}

			valBuf := make([]byte, 4)
			if _, err := io.ReadFull(conn, valBuf); err != nil {
				return
			}
			valLen := binary.BigEndian.Uint32(valBuf)
			if valLen > maxValLen {
				return
			}

			val := make([]byte, valLen)
			if _, err := io.ReadFull(conn, val); err != nil {
				return
			}

			returnpipe := make(chan Response, 1)
			pipe <- Payload{wayback: returnpipe, entry: Entry{key: string(key), val: val}}

			res, ok := <-returnpipe
			if !ok || !res.ok {
				conn.Write([]byte{0x00})
				return
			}

			if _, err := conn.Write([]byte{0x01}); err != nil {
				return
			}

		case 0x03:
			keyBuf := make([]byte, 2)
			if _, err := io.ReadFull(conn, keyBuf); err != nil {
				return
			}
			keyLen := binary.BigEndian.Uint16(keyBuf)
			if keyLen == 0 || int(keyLen) > maxKeyLen {
				return
			}

			key := make([]byte, keyLen)
			if _, err := io.ReadFull(conn, key); err != nil {
				return
			}

			returnpipe := make(chan Response, 1)
			pipe <- Payload{wayback: returnpipe, entry: Entry{key: string(key)}}

			res, ok := <-returnpipe
			if !ok || !res.ok {
				conn.Write([]byte{0x00})
				continue
			}

			out := make([]byte, 5+len(res.data))
			out[0] = 0x01
			binary.BigEndian.PutUint32(out[1:5], uint32(len(res.data)))
			copy(out[5:], res.data)

			if _, err := conn.Write(out); err != nil {
				return
			}

		default:
			return
		}
	}
}

func Processor(pipe chan Payload, ledger *[]byte, list map[string]ListEntry) {
	for data := range pipe {
		if data.entry.key == "" {
			data.wayback <- Response{ok: false}
			continue
		}

		if data.entry.val != nil {
			existing, exists := list[data.entry.key]
			newLen := len(data.entry.val)

			if exists && newLen <= existing.length {
				copy((*ledger)[existing.pos:], data.entry.val)
				list[data.entry.key] = ListEntry{pos: existing.pos, length: newLen}
			} else {
				startPos := len(*ledger)
				*ledger = append(*ledger, data.entry.val...)
				list[data.entry.key] = ListEntry{pos: startPos, length: newLen}
			}

			data.wayback <- Response{ok: true}
		} else {
			val, ok := list[data.entry.key]
			if !ok || val.pos+val.length > len(*ledger) {
				data.wayback <- Response{ok: false}
				continue
			}

			res := make([]byte, val.length)
			copy(res, (*ledger)[val.pos:val.pos+val.length])
			data.wayback <- Response{ok: true, data: res}
		}
	}
}
