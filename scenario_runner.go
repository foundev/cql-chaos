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
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Scenario interface {
	ExecuteUnit() error
	Init() error
}

//ScenarioResult tracks the total number of successes and failures
type ScenarioResult struct {
	Success                int64
	Errors                 int64
	AverageExecutionTimeMS float64
}

func (s *ScenarioResult) Report() string {
	return fmt.Sprintf("transaction report\n------------------\n%v successful %v failed\navg scenario transaction time: %.2f",
		s.Success,
		s.Errors,
		s.AverageExecutionTimeMS)
}

type ScenarioRunner struct {
	ThreadsInFlight  int64
	ProgressInterval int64
	Records          int64
	Verbose          bool
}

func (r *ScenarioRunner) Run(scene Scenario) ScenarioResult {
	var wg sync.WaitGroup

	var success int64
	var errors int64
	var totalExecutionTime int64
	var i int64
	for i = 0; i < r.Records; i++ {
		if i%r.ProgressInterval == 0 {
			//logging progress interval so we can see something is happening
			log.Printf("%v of %v records attempted", i+1, r.Records)
		}
		if i%r.ThreadsInFlight == 0 {
			//when we hit the maximum number of threads wait for these all to clear out before adding one more,
			//this is very naive and there are better ways to do this but it works for now
			wg.Wait()
		}
		wg.Add(1)
		go func() {
			t := time.Now()
			//if the scene unit fails add to the counter
			if err := scene.ExecuteUnit(); err != nil {
				if r.Verbose {
					log.Printf("ERROR - %v", err)
				}
				//using atomic since this is inside of a goroutine
				atomic.AddInt64(&errors, 1)
			} else {
				atomic.AddInt64(&success, 1)
			}
			d := time.Since(t)
			atomic.AddInt64(&totalExecutionTime, int64(d))
			wg.Done()
		}()
	}
	//clean up any dangling work
	wg.Wait()
	var executionTimeMS float64
	if totalExecutionTime > 0 {
		executionTimeMS = float64(totalExecutionTime) / float64(1000000)
	}
	return ScenarioResult{
		Success:                success,
		Errors:                 errors,
		AverageExecutionTimeMS: executionTimeMS / float64(i),
	}
}
