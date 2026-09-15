
# ramkv

in-memory key-value database built with go, raw tcp sockets, and a custom binary wire protocol. 

instead of using mutexes or wrapping standard text formats, it runs an actor loop where concurrent tcp workers feed incoming frames into a central processor over channels, backed by an append-only byte ledger and a hash index.

---

## how it works

```text
client connections
       |
       v
+-------------+      +-------------+
|  Worker 1   |      |  Worker 2   |   ... concurrent goroutines
+------+------+      +------+------+
       \                    /
        \                  /
   [Payload chan (datapipe, cap 50)]
                  |
                  v
       +--------------------+
       |   Processor Loop   |          ... single-threaded actor
       +---------+----------+
                 |
        +--------+--------+
        |                 |
        v                 v
  [Index Map]      [Byte Ledger]
map[string]Pos     []byte buffer
```

* **workers:** each connection runs in its own goroutine, parses binary frames off the wire, checks limits, and pushes operations into `datapipe`.
* **processor:** a single loop that pulls from `datapipe`. because only this loop touches the ledger and map, state updates require zero mutexes.
* **storage:** the ledger is an append-only `[]byte`. the index map stores the byte offset and length for each key.

---

## wire protocol

all commands use fixed-width headers and length-prefixed values in big-endian byte order.

### PING (`0x01`)
* **req:** `[0x01]` (1 byte)
* **res:** `[0x01]` (1 byte)

### SET (`0x02`)
* **req:** `[0x02]` (1B) + `[key_len]` (uint16) + `[key]` + `[val_len]` (uint32) + `[val]`
* **res:** `[0x01]` (ok) or `[0x00]` (err)

### GET (`0x03`)
* **req:** `[0x03]` (1B) + `[key_len]` (uint16) + `[key]`
* **res:** `[0x01]` (1B) + `[val_len]` (uint32) + `[val]`
* **res (missing/err):** `[0x00]` (1B)

---

## running & testing

```bash
# start the server
go run main.go
```

test script (`test.py`):

```python
import socket
import struct

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 5075))

# 1. PING
s.sendall(b'\x01')
assert s.recv(1) == b'\x01'

# 2. SET test = hello
key, val = b"test", b"hello"
frame = struct.pack(f'!BH{len(key)}sI{len(val)}s', 0x02, len(key), key, len(val), val)
s.sendall(frame)
assert s.recv(1) == b'\x01'

# 3. GET test
get_frame = struct.pack(f'!BH{len(key)}s', 0x03, len(key), key)
s.sendall(get_frame)
assert s.recv(1) == b'\x01'
val_len = struct.unpack('!I', s.recv(4))[0]
print("got:", s.recv(val_len).decode())

s.close()
```

---

## bottlenecks & current limitations

* **single processor bottleneck:** all operations (`GET` and `SET`) funnel into a single goroutine. reads are serialized sequentially behind writes instead of executing concurrently across available cpu cores.
* **unbounded memory growth:** updating an existing key with a longer value appends new bytes to the end of the `ledger` slice. old byte ranges are never reclaimed or compacted, causing memory usage to grow continuously on repeated writes.
* **per-op channel allocations:** every request makes a new unbuffered `returnpipe` channel (`make(chan Response, 1)`) inside the worker, generating heavy garbage collector pressure under high request volume.
* **no persistence:** the entire ledger sits in volatile ram. stopping the process drops all keys.
* **hard bounds:** keys are capped at 4096 bytes and values are capped at 16MB. frames exceeding these limits drop the connection immediately.
