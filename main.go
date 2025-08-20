package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	// "time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	concurrency := flag.Int("concurrency", 100, "concurrent workers")
	batchSize := flag.Int("batchsize", 10, "purchases per worker")
	flag.Parse()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		log.Fatal("DB_DSN env var is not set")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping db: %v", err)
	}

	db.SetMaxOpenConns(*concurrency)
	db.SetMaxIdleConns(*concurrency)

	// Initialize Schema
	initialStock := 10000000
	productID := 1
	if _, err := db.Exec("DROP TABLE IF EXISTS products"); err != nil {
		log.Fatalf("Failed to drop table: %v", err)
	}

	createTableSQL := "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(255), count BIGINT);"
	if _, err := db.Exec(createTableSQL); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	insertSQL := "INSERT INTO products (id, name, count) VALUES (?, ?, ?)"
	if _, err := db.Exec(insertSQL, productID, "T-Shirt", initialStock); err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}

	log.Printf("Starting: %d workers, %d purchases each...", *concurrency, *batchSize)

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < *batchSize; j++ {
				tx, err := db.Begin()
				if err != nil {
					continue
				}

				var currentStock int64
				// Lock the row for the entire transaction duration
				err = tx.QueryRow("SELECT count FROM products WHERE id = ? FOR UPDATE", productID).Scan(&currentStock)
				if err != nil {
					tx.Rollback()
					continue
				}

				// Simulate long-running business logic while holding the lock
				// time.Sleep(1 * time.Millisecond)

				if currentStock > 0 {
					_, err = tx.Exec("UPDATE products SET count = count - 1 WHERE id = ?", productID)
					if err != nil {
						tx.Rollback()
						continue
					}
				}

				if err := tx.Commit(); err != nil {
				    // If commit fails, the transaction is already rolled back by the driver.
					continue
				}
			}
		}(i + 1)
	}
	wg.Wait()
	log.Println("All workers finished.")

	var finalStock int64
	if err := db.QueryRow("SELECT count FROM products WHERE id = ?", productID).Scan(&finalStock); err != nil {
		log.Fatalf("Failed to query final stock: %v", err)
	}

	totalPurchases := *concurrency * *batchSize
	expectedStock := int64(initialStock) - int64(totalPurchases)

	fmt.Println("-----------------------------------------")
	fmt.Printf("Expected Final Stock: %d\n", expectedStock)
	fmt.Printf("Actual Final Stock:   %d\n", finalStock)
	fmt.Println("-----------------------------------------")

	if finalStock == expectedStock {
		log.Println("✅ Test successful! Data is consistent.")
	} else {
		log.Printf("❌ Test failed! Data is inconsistent. Final stock: %d, Expected: %d", finalStock, expectedStock)
	}
}
