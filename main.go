package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	dbURL = "postgresql://root@127.0.0.1:26257/defaultdb?sslmode=disable"
)

var (
	numParents       = flag.Int("np", 100, "number of inserts to parent table")
	numChildren      = flag.Int("nc", 100, "number of inserts to child table")
	maxChildAttempts = flag.Int("mc", 0, "maximum number of insert attempts for a child row; 0 indicates no maximum")
	concurrency      = flag.Int("g", 100, "max number of concurrent inserts to a table")
	numConns         = flag.Int("c", 100, "max number of connections")
	wait             = flag.Bool("w", false, "wait for all parents to be inserted before inserting children")
)

func main() {
	flag.Parse()

	// Read in connection string
	config, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		log.Fatal(err)
	}
	config.MaxConns = int32(*numConns)
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
	pStats := NewStats("p-inserts")
	execMany(pool, *numParents, 0, *concurrency, &wg, pStats, func(i int) (sql string) {
		return fmt.Sprintf("INSERT INTO p VALUES (%d, 'some text')", i)
	})

	if *wait {
		// Wait for the parents to finish inserting.
		wg.Wait()
	}

	cStats := NewStats("c-inserts")
	execMany(pool, *numChildren, *maxChildAttempts, *concurrency, &wg, cStats, func(i int) (sql string) {
		return fmt.Sprintf("INSERT INTO c VALUES (%d, %d, 'some text')", i, i)
	})

	wg.Wait()
	fmt.Println("done")
	fmt.Println(pStats.String())
	fmt.Println(cStats.String())
}

func mustExec(conn *pgxpool.Pool, sql string) {
	_, err := conn.Exec(context.Background(), sql)
	if err != nil {
		panic(err)
	}
}

func execMany(pool *pgxpool.Pool, times int, maxAttempts int, concurrency int, wg *sync.WaitGroup, s *Stats, genSQL func(i int) (sql string)) {
	if times <= 0 {
		return
	}
	for c := 0; c < concurrency; c++ {
		wg.Add(1)

		go func(c int) {
			var localStats Stats
			insertsCompleted := make([]bool, times/concurrency)
			insertAttempts := make([]int, times/concurrency)
			offset := c * (times / concurrency)
			for {
				for i := range insertAttempts {
					if insertsCompleted[i] {
						continue
					}
					if maxAttempts > 0 && insertAttempts[i] >= maxAttempts {
						insertsCompleted[i] = true
						continue
					}
					insertAttempts[i]++
					sql := genSQL(i + offset)
					localStats.Time(func() {
						if _, err := pool.Exec(context.Background(), sql); err == nil {
							insertsCompleted[i] = true
						}
					})
				}

				// Check if all inserts have succeeded.
				done := true
				for i := range insertsCompleted {
					if !insertsCompleted[i] {
						done = false
						break
					}
				}
				if done {
					break
				}
			}

			s.mu.Lock()
			s.Merge(&localStats)
			s.mu.Unlock()
			wg.Done()
		}(c)
	}
}

type Stats struct {
	name        string
	count       int
	totalMillis int64
	maxMillis   int64
	mu          sync.Mutex
}

func NewStats(name string) *Stats {
	return &Stats{name: name}
}

func (s *Stats) Time(fn func()) {
	start := time.Now()
	fn()
	d := time.Since(start).Milliseconds()
	s.totalMillis += d
	s.maxMillis = max(s.maxMillis, d)
	s.count++
}

func (s *Stats) Merge(other *Stats) {
	s.totalMillis += other.totalMillis
	s.count += other.count
	s.maxMillis = max(s.maxMillis, other.maxMillis)
}

func (s *Stats) String() string {
	if s.count == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(s.name)
	sb.WriteString(": ")
	sb.WriteString(fmt.Sprintf(
		"count=%d, avg_time=%dms, max_time=%dms",
		s.count, s.totalMillis/int64(s.count), s.maxMillis,
	))
	return sb.String()
}
