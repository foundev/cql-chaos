/**
  Copyright 2021 Ryan SVIHLA

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/
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
	help := flag.Bool("h", false, "show command help")
	hosts := flag.String("hosts", "localhost", "specify hosts separated by comma")
	port := flag.Int("port", 9042, "port to use to connect too")
	threads := flag.Int("threads", 100, "number of threads to use")
	records := flag.Int64("records", 1000000, "total number of records to write")
	progressInterval := flag.Int64("progressInterval", 10000, "every x records log progress")
	rf := flag.Int("rf", 1, "number of replicas to create if keyspace not present")
	scenario := flag.String("scenario", "default", "run a specified scenario; run -showScenarios to see all")
	verbose := flag.Bool("verbose", false, "turn on verbose output or not")

	flag.Parse()

	if *help {
		flag.Usage()
		return
	}
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

	scenarioRunner := ScenarioRunner{
		ThreadsInFlight:  int64(*threads),
		ProgressInterval: *progressInterval,
		Records:          *records,
		Verbose:          *verbose,
	}
	var scenarioRuntime Scenario
	switch *scenario {
	case "default":
		scenarioRuntime = &DefaultScenario{Session: session, RF: *rf}
	case "single-part":
		scenarioRuntime = &SinglePartScenario{Session: session, RF: *rf}
	case "high-cells-tiny-part":
		scenarioRuntime = &HighCellsTinyPartScenario{Session: session, PossibleIds: 100, RF: *rf}
	default:
		log.Printf("no scenario named %v was found", *scenario)
		flag.Usage()
	}
	result := scenarioRunner.Run(scenarioRuntime)
	fmt.Println(result.Report())
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
