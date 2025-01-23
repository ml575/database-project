# OwlDB Project

OwlDB NoSQL document database for COMP 318 class.

## Skeleton Code

### main

The provided `main.go` file is a simple skeleton for you to start
with. It handles gracefully closing the HTTP server when Ctrl-C is
pressed in the terminal that is running your program.  It does little
else.

### jsondata

The provided `jsondata` package provides a `JSONValue` type, a
`Visitor` interface and a few basic functions to work with JSON data.
You **must** use this package whenever you access the contents of a
JSON document in your program.

### logger

The provided `logger` package provides a structured logger based on
the standard `log/slog` package that allows you to set the log level
and colorize the output.

## Build

Assuming you have a file "document.json" that holds your desired
document schema and a file "tokens.json" that holds a set of tokens,
then you could run your program like so:

```./database-project -s document.json -t tokens.json -p 3318```

Note that you can always run your program without building it first as
follows:

```go run main.go -s document.json -t tokens.json -p 3318```

