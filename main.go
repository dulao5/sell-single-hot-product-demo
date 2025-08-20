package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	numProducts    = 10
	initialStock   = 10000000
)

func main() {
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

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

	// Initialize Schema with multiple products
	log.Println("Initializing schema for multiple products...")
	if _, err := db.Exec("DROP TABLE IF EXISTS products"); err != nil {
		log.Fatalf("Failed to drop table: %v", err)
	}
	createTableSQL := "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(255), count BIGINT);"
	if _, err := db.Exec(createTableSQL); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	insertSQL := "INSERT INTO products (id, name, count) VALUES (?, ?, ?)"
	for i := 1; i <= numProducts; i++ {
		productName := fmt.Sprintf("T-Shirt-%d", i)
		if _, err := db.Exec(insertSQL, i, productName, initialStock); err != nil {
			log.Fatalf("Failed to insert data for product %d: %v", i, err)
		}
	}
	log.Printf("Initialized %d products.", numProducts)

	log.Printf("Starting: %d workers, %d purchases each...", *concurrency, *batchSize)

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < *batchSize; j++ {
				// Each transaction targets a random product
				productID := rand.Intn(numProducts) + 1

				tx, err := db.Begin()
				if err != nil {
					continue
				}

				var currentStock int64
				err = tx.QueryRow("SELECT count FROM products WHERE id = ? FOR UPDATE", productID).Scan(&currentStock)
				if err != nil {
					tx.Rollback()
					continue
				}

				// time.Sleep(500 * time.Millisecond)

				if currentStock > 0 {
					_, err = tx.Exec("UPDATE products SET count = count - 1 WHERE id = ?", productID)
					if err != nil {
						tx.Rollback()
						continue
					}
				}

				if err := tx.Commit(); err != nil {
					continue
				}
			}
		}(i + 1)
	}
	wg.Wait()
	log.Println("All workers finished.")

	// Verify the final total stock
	var finalTotalStock int64
	if err := db.QueryRow("SELECT SUM(count) FROM products").Scan(&finalTotalStock); err != nil {
		log.Fatalf("Failed to query final total stock: %v", err)
	}

	totalPurchases := *concurrency * *batchSize
	initialTotalStock := int64(initialStock) * int64(numProducts)
	expectedTotalStock := initialTotalStock - int64(totalPurchases)

	fmt.Println("-----------------------------------------")
	fmt.Printf("Initial Total Stock: %d\n", initialTotalStock)
	fmt.Printf("Expected Total Stock: %d\n", expectedTotalStock)
	fmt.Printf("Actual Total Stock:   %d\n", finalTotalStock)
	fmt.Println("-----------------------------------------")

	if finalTotalStock == expectedTotalStock {
		log.Println("✅ Test successful! Data is consistent.")
	} else {
		log.Printf("❌ Test failed! Data is inconsistent. Final stock: %d, Expected: %d", finalTotalStock, expectedTotalStock)
	}
}
