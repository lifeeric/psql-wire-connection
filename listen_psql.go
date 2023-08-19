package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

var (
	sourceDB   *sql.DB
	destDB     *sql.DB
	bufferSize int = 1000
	timeout    time.Duration = 30 * time.Second
	buffers    map[string][]string
	mutexes    map[string]*sync.Mutex
)

func main() {
	initDatabases()
	initBuffers()

	listener, err := net.Listen("tcp", "localhost:5432")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	log.Println("Listening for connections...")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleConnection(conn)
	}
}

func initDatabases() {
	var err error
	sourceDB, err = sql.Open("postgres", "source_connection_string_here")
	if err != nil {
		log.Fatal(err)
	}

	destDB, err = sql.Open("postgres", "destination_connection_string_here")
	if err != nil {
		log.Fatal(err)
	}
}

func initBuffers() {
	buffers = make(map[string][]string)
	mutexes = make(map[string]*sync.Mutex)
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 4096)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			log.Println(err)
			return
		}

		query := string(buffer[:n])
		parseQuery(query)
	}
}

func parseQuery(query string) {
	parts := strings.Split(query, " ")
	if len(parts) < 4 || strings.ToLower(parts[0]) != "insert" {
		return
	}

	tableName := parts[2]
	// columns := parts[3]
	values := parts[4]

	mutex := getMutex(tableName)
	mutex.Lock()
	defer mutex.Unlock()

	buffers[tableName] = append(buffers[tableName], fmt.Sprintf("(%s)", values))

	if len(buffers[tableName]) >= bufferSize {
		insertBatch(tableName)
	}
}

func getMutex(tableName string) *sync.Mutex {
	if mutexes[tableName] == nil {
		mutexes[tableName] = &sync.Mutex{}
	}
	return mutexes[tableName]
}

func insertBatch(tableName string) {
	mutex := getMutex(tableName)
	mutex.Lock()
	defer mutex.Unlock()

	if len(buffers[tableName]) > 0 {
		query := fmt.Sprintf("INSERT INTO %s %s VALUES %s;",
			tableName,
			strings.Join(strings.Fields(buffers[tableName][0]), ","),
			strings.Join(buffers[tableName], ","))

		_, err := destDB.Exec(query)
		if err != nil {
			log.Println(err)
		}

		// Clear the buffer.
		buffers[tableName] = nil
	}
}
