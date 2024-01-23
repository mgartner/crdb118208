# repro 118208

This is an attempted reproduction for
https://github.com/cockroachdb/cockroach/issues/118208.

To run:
1. Start a CRDB server: `cockroach demo --empty --insecure`.
1. Build the binary: `go build .`.
1. Run the binary: `./repro 5000`.
