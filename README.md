# repro 118208

This is an attempted reproduction for
https://github.com/cockroachdb/cockroach/issues/118208.

To run:
1. Start a CRDB server: `cockroach demo --empty --insecure`.
1. Build the binary: `go build .`.
1. Run the binary: `./repro -n 1000 -g 1000 -c 1000`.
