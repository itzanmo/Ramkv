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
