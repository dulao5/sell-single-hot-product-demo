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
	// initialStock is now the stock per product.
	initialStock = 10000000
)

func main() {
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// --- Configuration Flags ---
	concurrency := flag.Int("concurrency", 100, "Number of concurrent purchase workers")
        batchSize := flag.Int("batchsize", 10, "Number of purchases per worker")
	numProducts := flag.Int("products", 1, "Number of distinct products (rows) to simulate")
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

	// --- Schema Initialization ---
	log.Printf("Initializing schema for %d products...", *numProducts)
	if _, err := db.Exec("DROP TABLE IF EXISTS products"); err != nil {
		log.Fatalf("Failed to drop table: %v", err)
	}
	createTableSQL := "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(255), count BIGINT);"
	if _, err := db.Exec(createTableSQL); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	insertSQL := "INSERT INTO products (id, name, count) VALUES (?, ?, ?)"
	for i := 1; i <= *numProducts; i++ {
		productName := fmt.Sprintf("T-Shirt-%d", i)
		if _, err := db.Exec(insertSQL, i, productName, initialStock); err != nil {
			log.Fatalf("Failed to insert data for product %d: %v", i, err)
		}
	}
	log.Printf("Initialized %d products.", *numProducts)

	// --- Simulation ---
	log.Printf("Starting: %d workers, %d purchases each, across %d products...", *concurrency, *batchSize, *numProducts)

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < *batchSize; j++ {
				productID := rand.Intn(*numProducts) + 1

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

	// --- Verification ---
	var finalTotalStock int64
	if err := db.QueryRow("SELECT SUM(count) FROM products").Scan(&finalTotalStock); err != nil {
		log.Fatalf("Failed to query final total stock: %v", err)
	}

	totalPurchases := *concurrency * *batchSize
	initialTotalStock := int64(initialStock) * int64(*numProducts)
	expectedTotalStock := initialTotalStock - int64(totalPurchases)

	fmt.Println("-----------------------------------------")
	fmt.Printf("Products:             %d\n", *numProducts)
	fmt.Printf("Initial Total Stock:  %d\n", initialTotalStock)
	fmt.Printf("Expected Total Stock: %d\n", expectedTotalStock)
	fmt.Printf("Actual Total Stock:   %d\n", finalTotalStock)
	fmt.Println("-----------------------------------------")

	if finalTotalStock == expectedTotalStock {
		log.Println("✅ Test successful! Data is consistent.")
	} else {
		log.Printf("❌ Test failed! Data is inconsistent. Final stock: %d, Expected: %d", finalTotalStock, expectedTotalStock)
	}
}
