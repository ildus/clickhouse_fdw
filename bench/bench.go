package main

import (
	"flag"
	"fmt"
	"github.com/ildus/pqt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	connections        = flag.Int("connections", 0, "Connections count (by default number of cores)")
	bench_time         = flag.Int("bench_time", 60, "Bench time (seconds)")
	counter     uint64 = 0
)

const (
	initinit = "CREATE EXTENSION clickhouse_fdw;"

	initscript = `
DROP SCHEMA IF EXISTS clickhouse CASCADE;
DROP SERVER IF EXISTS loopback CASCADE;
CREATE SERVER loopback FOREIGN DATA WRAPPER clickhouse_fdw
    OPTIONS(dbname 'regression', driver '%s');
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;

SELECT clickhousedb_raw_query('DROP DATABASE IF EXISTS regression');
SELECT clickhousedb_raw_query('CREATE DATABASE regression');

-- integer types
SELECT clickhousedb_raw_query('CREATE TABLE regression.ints (
    c1 Int8, c2 Int16, c3 Int32, c4 Int64,
    c5 UInt8, c6 UInt16, c7 UInt32, c8 UInt64,
    c9 Float32, c10 Float64
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');
SELECT clickhousedb_raw_query('INSERT INTO regression.ints SELECT
    number, number + 1, number + 2, number + 3, number + 4, number + 5,
    number + 6, number + 7, number + 8.1, number + 9.2 FROM numbers(10);');

-- date and string types
SELECT clickhousedb_raw_query('CREATE TABLE regression.types (
    c1 Date, c2 DateTime, c3 String, c4 FixedString(5), c5 UUID,
    c6 Enum8(''one'' = 1, ''two'' = 2),
    c7 Enum16(''one'' = 1, ''two'' = 2, ''three'' = 3)
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');
SELECT clickhousedb_raw_query('INSERT INTO regression.types SELECT
    addDays(toDate(''1990-01-01''), number),
    addMinutes(addSeconds(addDays(toDateTime(''1990-01-01 10:00:00''), number), number), number),
    format(''number {0}'', toString(number)),
    format(''num {0}'', toString(number)),
    format(''f4bf890f-f9dc-4332-ad5c-0c18e73f28e{0}'', toString(number)),
    ''two'',
    ''three''
    FROM numbers(10);');

-- array types
SELECT clickhousedb_raw_query('CREATE TABLE regression.arrays (
    c1 Array(Int), c2 Array(String)
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');
SELECT clickhousedb_raw_query('INSERT INTO regression.arrays SELECT
    [number, number + 1],
    [format(''num{0}'', toString(number)), format(''num{0}'', toString(number + 1))]
    FROM numbers(10);');

SELECT clickhousedb_raw_query('CREATE TABLE regression.tuples (
    c1 Int8,
    c2 Tuple(Int, String, Float32)
) ENGINE = MergeTree PARTITION BY c1 ORDER BY (c1);
');
SELECT clickhousedb_raw_query('INSERT INTO regression.tuples SELECT
    number,
    (number, toString(number), number + 1.0)
    FROM numbers(10);');

CREATE SCHEMA clickhouse;
IMPORT FOREIGN SCHEMA "ok" FROM SERVER loopback INTO clickhouse;
`
)

var (
	queries = []string{
		"SELECT * FROM clickhouse.tuples;",
		"SELECT * FROM clickhouse.arrays;",
		"SELECT * FROM clickhouse.types;",
		"SELECT * FROM clickhouse.ints;",
	}
)

func listener(wg *sync.WaitGroup, chm chan int, node *pqt.PostgresNode) {
	defer wg.Done()

	conn := pqt.MakePostgresConn(node, "postgres")
	defer conn.Close()

	for query_idx := range chm {
		sql := queries[query_idx]
		rows := conn.Fetch(sql)
		defer rows.Close()

		for rows.Next() {
		}
		atomic.AddUint64(&counter, 1)
	}
}

func processQueries(node *pqt.PostgresNode, init_sql string, title string) {
	var wg sync.WaitGroup
	chm := make(chan int, 1)

	node.Execute("postgres", init_sql)
	for i := 0; i < *connections; i++ {
		wg.Add(1)
		go listener(&wg, chm, node)
	}

	start_time := time.Now()
	for {
		for i := 0; i < len(queries); i++ {
			chm <- i

			if time.Since(start_time) > (time.Duration(*bench_time) * time.Second) {
				goto end
			}
		}
	}
end:
	close(chm)
	log.Printf("Processed in %s mode: %d", title, atomic.LoadUint64(&counter))
	wg.Wait()
}

func main() {
	var node *pqt.PostgresNode

	defer func() {
		if node != nil {
			node.Stop()
		}

		if err := recover(); err != nil {
			log.Fatal("Program is exiting with exception: ", err)
		}
	}()

	go func() {
		sigchan := make(chan os.Signal, 10)
		signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)
		<-sigchan
		log.Println("Got interrupt signal. Exited")
		os.Exit(0)
	}()

	flag.Parse()

	node = pqt.MakePostgresNode("master")
	node.Init()
	node.AppendConf("postgresql.conf", "log_statement=none")
	node.Start()
	node.Execute("postgres", initinit)

	if *connections <= 0 {
		*connections = runtime.NumCPU()
	}

	log.Println("Number of connections: ", *connections)

	processQueries(node, fmt.Sprintf(initscript, "http"), "http")
	processQueries(node, fmt.Sprintf(initscript, "binary"), "binary")
}
