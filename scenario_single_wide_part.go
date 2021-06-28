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

type SinglePartScenario struct {
	Session *gocql.Session
	PK      int32
	RF      int
}

func (d *SinglePartScenario) Init() error {
	err := d.Session.Query(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS high_cells WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %v }", d.RF)).Exec()
	if err != nil {
		return fmt.Errorf("unable to create keyspace high_cells with error %v", err)
	}
	err = d.Session.Query("CREATE TABLE IF NOT EXISTS high_cells.testers (id int, values int, counter int, PRIMARY KEY(id, values))").Exec()
	if err != nil {
		return fmt.Errorf("unable to create table high_cells.testers with error %v", err)
	}
	err = d.Session.Query("CREATE SEARCH INDEX IF NOT EXISTS on high_cells.testers").Exec()
	if err != nil {
		return fmt.Errorf("unable to create search index on high_cells.testers with error %v", err)
	}
	d.PK = rand.Int31()
	return nil
}

func (d *SinglePartScenario) ExecuteUnit() error {
	return d.Session.Query(`INSERT INTO high_cells.testers (id, values, counter) VALUES (?, ?, ?)`,
		d.PK, rand.Int31(), rand.Int31()).Exec()
}
