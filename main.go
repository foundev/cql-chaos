package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"

	"github.com/gocql/gocql"
)

func main() {
	hosts := flag.String("hosts", "localhost", "specify hosts separated by comma")
	port := flag.Int("port", 9042, "port to use to connect too")
	threads := flag.Int("threads", 8, "number of threads to use")
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
	results := make(chan error, *threads)
	var i int64
	for i = 0; i < *records; i++ {
		go func() {
			results <- session.Query(`INSERT INTO test.testers (id, values, counter) VALUES (?, ?, ?)`,
				rand.Int31(), randomStr, rand.Int31()).Exec()
		}()
	}
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
