package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gocql/gocql"
)

func main() {
	hosts := flag.String("hosts", "localhost", "specify hosts separated by comma")
	port := flag.Int("port", 9042, "port to use to connect too")
	threads := flag.Int("threads", 100, "number of threads to use")
	records := flag.Int64("records", 1000000, "total number of records to write")
	flag.Parse()
	rawHosts := strings.Split(*hosts, ",")

	var contactPoints []string
	for _, host := range rawHosts {
		contactPoint := fmt.Sprintf("%v:%v", strings.TrimSpace(host), *port)
		contactPoints = append(contactPoints, contactPoint)
	}
	cluster := gocql.NewCluster(contactPoints...)
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup

	var success int64
	var errors int64
	threadsFlight := int64(*threads)
	progressInterval := int64(10000)
	var i int64
	for i = 0; i < *records; i++ {
		if i%progressInterval == 0 {
			log.Printf("%v of %v records attempted", i+1, *records)
		}
		if i%threadsFlight == 0 {
			wg.Wait()
		}
		wg.Add(1)
		go func() {
			if err = session.Query(`INSERT INTO test.testers (id, values, counter) VALUES (?, ?, ?)`,
				rand.Int31(), randomStr(), rand.Int31()).Exec(); err != nil {
				log.Printf("ERROR - %v", err)
				atomic.AddInt64(&errors, 1)
			} else {
				atomic.AddInt64(&success, 1)
			}
			wg.Done()
		}()
	}
	log.Printf("%v successful %v failed", success, errors)
}

func randomStr() string {
	chars := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ" +
		"abcdefghijklmnopqrstuvwxyzåäö" +
		"0123456789")
	length := 8
	var b strings.Builder
	for i := 0; i < length; i++ {
		b.WriteRune(chars[rand.Intn(len(chars))])
	}
	return b.String()
}
