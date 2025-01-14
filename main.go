package main

import (
	"database/sql"
	"fmt"
	"net/http"
	_ "github.com/lib/pq"
)

var (
	DSN = "user=postgres password=********** dbname=db_go sslmode=disable"
)

func main() {
	db, err := sql.Open("postgres", DSN)
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	handler, err := NewDbExplorer(db)
	if err != nil {
		panic(err)
	}

	defer db.Close()

	fmt.Println("starting server at :8082")
	http.ListenAndServe(":8082", handler)
}
