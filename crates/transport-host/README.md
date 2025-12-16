# wRPC Host Transport

This crate allows creating host-defined transports. This is useful for constrained environments (ex: browsers) that may not be able to use other transports.

## Architecture

TODO: can we "wait" for a channel to get data (from rust) if the channel came from the host?
```
resource HostChannel {
  read: func() -> stream<list<u8>>;
  write: func(data: list<u8>) -> future<()>;
}
interface Foo {
  createChannel: func() -> HostChannel
}
```