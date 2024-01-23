package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"os"
	"strconv"
	"sync"
)

const (
	dbURL = "postgresql://root@127.0.0.1:26257/defaultdb?sslmode=disable"
)

func main() {
	flag.Parse()
	numInserts, err := strconv.Atoi(flag.Arg(0))
	if err != nil {
		fmt.Println("usage: ./repro <number of inserts>\nexample: ./repro 500")
		os.Exit(0)
	}

	// Read in connection string
	config, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		log.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create connection pool: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Setup tables.
	mustExec(pool, "DROP TABLE IF EXISTS c")
	mustExec(pool, "DROP TABLE IF EXISTS p")
	mustExec(pool, "CREATE TABLE p (id INT PRIMARY KEY, t TEXT)")
	mustExec(pool, "CREATE TABLE c (id INT PRIMARY KEY, p_id INT NOT NULL REFERENCES p(id) ON DELETE CASCADE, t TEXT)")

	// INSERT into p and c in parallel.
	fmt.Println("starting inserts...")
	var wg sync.WaitGroup
	execMany(pool, numInserts, &wg, func(i int) (sql string) {
		return fmt.Sprintf("INSERT INTO p VALUES (%d, 'some text')", i)
	})

	execMany(pool, numInserts, &wg, func(i int) (sql string) {
		return fmt.Sprintf("INSERT INTO c VALUES (%d, %d, 'some text')", i, i)
	})

	wg.Wait()
	fmt.Println("done")
}

func mustExec(conn *pgxpool.Pool, sql string) {
	_, err := conn.Exec(context.Background(), sql)
	if err != nil {
		panic(err)
	}
}

func execMany(pool *pgxpool.Pool, times int, wg *sync.WaitGroup, genSQL func(i int) (sql string)) {
	const concurrency = 4
	for c := 0; c < concurrency; c++ {
		wg.Add(1)
		go func(c int) {
			inserted := make([]bool, times/concurrency)
			offset := c * (times / concurrency)
			for {
				for i := range inserted {
					sql := genSQL(i + offset)
					if _, err := pool.Exec(context.Background(), sql); err != nil {
						inserted[i] = true
					}
				}

				// Check if all inserts have succeeded.
				done := true
				for i := range inserted {
					if !inserted[i] {
						done = false
						break
					}
				}
				if done {
					break
				}
			}
			wg.Done()
		}(c)
	}
}
