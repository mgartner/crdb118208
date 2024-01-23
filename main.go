package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v5"
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
	config, err := pgx.ParseConfig(dbURL)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(context.Background())

	// Setup tables.
	mustExec(conn, "DROP TABLE IF EXISTS c")
	mustExec(conn, "DROP TABLE IF EXISTS p")
	mustExec(conn, "CREATE TABLE p (id INT PRIMARY KEY, t TEXT)")
	mustExec(conn, "CREATE TABLE c (id INT PRIMARY KEY, p_id INT NOT NULL REFERENCES p(id) ON DELETE CASCADE, t TEXT)")

	fmt.Println("starting inserts...")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		conn, err := pgx.ConnectConfig(context.Background(), config)
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close(context.Background())

		execMany(conn, numInserts, func(i int) (sql string) {
			return fmt.Sprintf("INSERT INTO p VALUES (%d, 'some text')", i)
		})
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		conn, err := pgx.ConnectConfig(context.Background(), config)
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close(context.Background())

		execMany(conn, numInserts, func(i int) (sql string) {
			return fmt.Sprintf("INSERT INTO c VALUES (%d, %d, 'some text')", i, i)
		})
		wg.Done()
	}()

	wg.Wait()
	fmt.Println("done")
}

func mustExec(conn *pgx.Conn, sql string) {
	_, err := conn.Exec(context.Background(), sql)
	if err != nil {
		panic(err)
	}
}

func execMany(conn *pgx.Conn, times int, genSQL func(i int) (sql string)) {
	inserted := make([]bool, times)
	for {
		for i := range inserted {
			sql := genSQL(i)
			if _, err := conn.Exec(context.Background(), sql); err != nil {
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
}
