# Ramkv
a minimal, actor-based in-memory key-value store built over raw tcp using a custom binary wire protocol.  instead of wrapping standard json/text parsers or relying on global lock contention, `ramkv` uses a dedicated single-threaded state machine (`Processor`) that communicates with concurrent network worker goroutines over buffered channels
