// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"golang.org/x/net/context"
)

var (
	host        = flag.String("host", "127.0.0.1", "TiDB host")
	port        = flag.Int("port", 4000, "TiDB port")
	user        = flag.String("user", "root", "TiDB user")
	password    = flag.String("password", "", "TiDB password")
	concurrency = flag.Int("concurrent", 10000, "Concurrency")
	numAccounts = flag.Int("num-accts", 100000000, "number of accounts")
)


func mustExec(db *sql.DB, query string, args ...interface{}) sql.Result {
	r, err := db.Exec(query, args...)
	if err != nil {
		log.Fatalf("exec %s err %v", query, err)
	}
	return r
}

// BackCase is for concurrent balance transfer.
type BankCase struct {
}

// Initialize implements Case Initialize interface.
func (c *BankCase) Initialize(ctx context.Context, db *sql.DB) error {
	isDropped := c.tryDrop(db)
	if !isDropped {
		c.startVerify(ctx, db)
		return nil
	}

	mustExec(db, "create table if not exists accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)")
	var wg sync.WaitGroup

	batchSize := 100
	jobCount := *numAccounts / batchSize
	wg.Add(jobCount)

	ch := make(chan int, jobCount)
	for i := 0; i < *concurrency; i++ {
		go func() {
			args := make([]string, batchSize)

			for {
				startIndex, ok := <-ch
				if !ok {
					return
				}

				start := time.Now()

				for i := 0; i < batchSize; i++ {
					args[i] = fmt.Sprintf("(%d, %d)", startIndex+i, 1000)
				}
				mustExec(db, fmt.Sprintf("INSERT INTO accounts (id, balance) VALUES %s", strings.Join(args, ",")))

				log.Infof("insert %d accounts, takes %s", batchSize, time.Now().Sub(start))

				wg.Done()
			}
		}()
	}

	for i := 0; i < jobCount; i++ {
		ch <- i * batchSize
	}

	wg.Wait()
	close(ch)

	c.startVerify(ctx, db)
	return nil
}

func (c *BankCase) startVerify(ctx context.Context, db *sql.DB) {
	c.verify(db)

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.verify(db)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Execute implements Case Execute interface.
func (c *BankCase) Execute(ctx context.Context, db *sql.DB, index int) error {
	c.moveMoney(db)
	return nil
}

// String implements fmt.Stringer interface.
func (c *BankCase) String() string {
	return "bank"
}

//tryDrop will drop table if data incorrect and panic error likes Bad connect.
func (c *BankCase) tryDrop(db *sql.DB) bool {
	var count int
	var table string
	//if table is not exist ,return true directly
	query := "show tables like 'accounts'"
	err := db.QueryRow(query).Scan(&table)
	switch {
	case err == sql.ErrNoRows:
		return true
	case err != nil:
		log.Fatal(err)
	}

	query = "select count(*) as count from accounts"
	err = db.QueryRow(query).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	if count == *numAccounts {
		return false
	}

	log.Infof("we need %d accounts but got %d, re-initialize the data again", *numAccounts, count)
	mustExec(db, "drop table if exists accounts")
	return true
}

func (c *BankCase) verify(db *sql.DB) {
	var total int

	query := "select sum(balance) as total from accounts"
	err := db.QueryRow(query).Scan(&total)
	if err != nil {
		return
	}

	check := *numAccounts * 1000
	if total != check {
		log.Fatalf("[%s] total must %d, but got %d", c, check, total)
	}
}

func (c *BankCase) moveMoney(db *sql.DB) {
	var from, to int
	for {
		from, to = rand.Intn(*numAccounts), rand.Intn(*numAccounts)
		if from == to {
			continue
		}
		break
	}

	amount := rand.Intn(999)
	c.execTransaction(db, from, to, amount)
}

func (c *BankCase) execTransaction(db *sql.DB, from, to int, amount int) error {
	tx, err := db.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	defer tx.Rollback()

	rows, err := tx.Query(fmt.Sprintf("SELECT id, balance FROM accounts WHERE id IN (%d, %d) FOR UPDATE", from, to))
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	var (
		fromBalance int
		toBalance   int
		count       int = 0
	)

	for rows.Next() {
		var id, balance int
		if err = rows.Scan(&id, &balance); err != nil {
			return errors.Trace(err)
		}
		switch id {
		case from:
			fromBalance = balance
		case to:
			toBalance = balance
		default:
			log.Fatalf("got unexpected account %d", id)
		}

		count += 1
	}

	if err = rows.Err(); err != nil {
		return errors.Trace(err)
	}

	if count != 2 {
		log.Fatalf("select %d(%d) -> %d(%d) invalid count %d", from, fromBalance, to, toBalance, count)
	}

	if fromBalance >= amount {
		update := fmt.Sprintf(`
UPDATE accounts
  SET balance = CASE id WHEN %d THEN %d WHEN %d THEN %d END
  WHERE id IN (%d, %d)
`, to, toBalance+amount, from, fromBalance-amount, from, to)
		_, err = tx.Exec(update)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return errors.Trace(tx.Commit())
}

func openDB() (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/test", *user, *password, *host, *port)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(*concurrency)
	return db, nil
}

func main() {
	flag.Parse()

	// Create the database
	db, err := openDB()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("Got signal [%d] to exit.", sig)

		db.Close()
		cancel()
	}()

	var wg sync.WaitGroup
	wg.Add(*concurrency)

	bank := &BankCase{}

	bank.Initialize(ctx, db)

	// Run all cases
	for i := 0; i < *concurrency; i++ {
		go func(i int) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
					bank.Execute(ctx, db, i)
				}
			}
		}(i)
	}

	wg.Wait()
}
