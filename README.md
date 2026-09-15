```markdown
# ramkv

a minimal, actor-based in-memory key-value store built over raw tcp using a custom binary wire protocol.

instead of wrapping standard json/text parsers or relying on global lock contention, `ramkv` uses a dedicated single-threaded state machine (`Processor`) that communicates with concurrent network worker goroutines over buffered channels, paired with an append-only log-indexed memory ledger.

---

## architecture

```text
               +-------------------------------------------+
               |                 ramkv                     |
               |                                           |
[client 1] <-> | [Worker 1] ---\                           |
               |                +--> [datapipe chan]       |
[client 2] <-> | [Worker 2] ------->       |               |
               |                           v               |
               |                   [Processor Loop]        |
               |                     |          |          |
               |         (key index) v          v (ledger) |
               |        map[string]Pos       []byte buf    |
               +-------------------------------------------+
```

* **network workers (`Worker`):** one goroutine per connection. handles frame parsing off the wire, enforces payload limits, allocates a transient return channel, and hands off tasks.
* **state manager (`Processor`):** acts as an actor. it exclusively owns the state ledger and index map, processing commands sequentially. this guarantees zero race conditions on state mutations without requiring mutexes.
* **storage layout:** keys are indexed in an in-memory hash map (`map[string]ListEntry`) storing byte offsets and lengths pointing into a contiguous `[]byte` ledger.

---

## wire protocol

all frames are binary, fixed-header, length-prefixed streams using network byte order (big-endian).

### 1. PING (`0x01`)
health check.

* **request:**
  ```text
  +---------------+
  | OpCode (0x01) |
  |    (1 byte)   |
  +---------------+
  ```
* **response:**
  ```text
  +---------------+
  | Status (0x01) |
  |    (1 byte)   |
  +---------------+
  ```

---

### 2. SET (`0x02`)
writes or updates a key.

* **request:**
  ```text
  +--------+------------+---------------+------------+-----------------+
  |  0x02  |  Key Len   |      Key      |  Val Len   |      Value      |
  |  (1B)  | (uint16 BE)|   (N bytes)   | (uint32 BE)|    (M bytes)    |
  +--------+------------+---------------+------------+-----------------+
  ```
* **response:**
  ```text
  +---------------+
  | Status (1B)   |  -> 0x01 = OK, 0x00 = Error
  +---------------+
  ```

---

### 3. GET (`0x03`)
fetches a key.

* **request:**
  ```text
  +--------+------------+---------------+
  |  0x03  |  Key Len   |      Key      |
  |  (1B)  | (uint16 BE)|   (N bytes)   |
  +--------+------------+---------------+
  ```
* **response (success):**
  ```text
  +--------+------------+-----------------+
  |  0x01  |  Val Len   |      Value      |
  |  (1B)  | (uint32 BE)|    (M bytes)    |
  +--------+------------+-----------------+
  ```
* **response (not found / error):**
  ```text
  +--------+
  |  0x00  |
  |  (1B)  |
  +--------+
  ```

---

## quick start

### run the server

```bash
go run main.go
# listening on :5075
```

### test with python

save this as `test_client.py`:

```python
import socket
import struct

def test_db():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 5075))

    # 1. PING
    s.sendall(b'\x01')
    pong = s.recv(1)
    assert pong == b'\x01', f"ping failed: {pong}"
    print("[+] PING: OK")

    # 2. SET "foo" = "bar"
    key = b"foo"
    val = b"bar"
    # Op(0x02) + KeyLen(uint16) + Key + ValLen(uint32) + Val
    payload = struct.pack(f'!BH{len(key)}sI{len(val)}s', 0x02, len(key), key, len(val), val)
    s.sendall(payload)
    res = s.recv(1)
    assert res == b'\x01', f"set failed: {res}"
    print("[+] SET: OK")

    # 3. GET "foo"
    # Op(0x03) + KeyLen(uint16) + Key
    get_payload = struct.pack(f'!BH{len(key)}s', 0x03, len(key), key)
    s.sendall(get_payload)
    status = s.recv(1)
    assert status == b'\x01', "key not found"
    val_len = struct.unpack('!I', s.recv(4))[0]
    out_val = s.recv(val_len)
    print(f"[+] GET: {out_val.decode('utf-8')}")

    s.close()

if __name__ == "__main__":
    test_db()
```

run the script:

```bash
python3 test_client.py
```

---

## design trade-offs & limitations

* **actor serialization vs read parallelism:** routing all traffic through `Processor` avoids mutex locks entirely, but sequential execution means heavy `GET` queries can back up writes. a future refactor could use a sharded `sync.RWMutex` map for concurrent reads.
* **in-memory log fragmentation:** updating a key with a larger payload appends to the tail of the slice, leaving stale bytes behind in `ledger`. because it's RAM-backed, memory grows until process termination unless a compaction routine runs.
* **per-request channel overhead:** each worker command instantiates a transient `returnpipe` channel for synchronization. reusing channels or moving to connection-bound queues would drop GC pressure under high RPS.
```
