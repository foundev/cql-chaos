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
	"math/rand"

	"github.com/gocql/gocql"
)

type HighCellsTinyPartScenario struct {
	Session     *gocql.Session
	PossibleIds int
	RF          int
}

func (d *HighCellsTinyPartScenario) Init() error {
	err := d.Session.Query(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %v }", d.RF)).Exec()
	if err != nil {
		return fmt.Errorf("unable to create keyspace high_cells with error %v", err)
	}
	err = d.Session.Query("CREATE TABLE IF NOT EXISTS high_cells.testers (id uuid, values int, counter int, PRIMARY KEY(id, values))").Exec()
	if err != nil {
		return fmt.Errorf("unable to create table high_cells.testers with error %v", err)
	}
	err = d.Session.Query("CREATE SEARCH INDEX IF NOT EXISTS on high_cells.testers").Exec()
	if err != nil {
		return fmt.Errorf("unable to create search index on high_cells.testers with error %v", err)
	}
	return nil
}

func (d *HighCellsTinyPartScenario) ExecuteUnit() error {
	pk := rand.Intn(d.PossibleIds)
	return d.Session.Query(`INSERT INTO high_cells.testers (id, values, counter) VALUES (?, ?, ?)`,
		pk, rand.Int31(), rand.Int31()).Exec()
}
