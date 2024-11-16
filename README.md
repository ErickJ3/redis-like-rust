# Rust Redis-like Server

A lightweight Redis-like server implementation in Rust supporting basic key-value operations with optional expiration times. The server implements a subset of the RESP (Redis Serialization Protocol) and handles concurrent connections using Tokio.

## Features

- Basic Redis commands (PING, ECHO, GET, SET)
- Key expiration with PX option
- Thread-safe concurrent access
- RESP protocol support
- Automatic cleanup of expired keys

## Commands

### PING
Returns PONG. Used for connection testing.
```
> PING
< PONG
```

### ECHO
Returns the given message.
```
> ECHO message
< message
```

### SET
Stores a key-value pair, optionally with expiration time in milliseconds.
```
> SET key value
< OK

> SET key value PX 1000  # Expires after 1 second
< OK
```

### GET
Retrieves the value for a given key.
```
> GET key
< value

> GET nonexistent
< (nil)
```