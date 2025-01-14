package main

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"time"

	_ "github.com/lib/pq"
)

// CaseResponse
type CR map[string]interface{}

type Case struct {
	Method string
	Path   string
	Query  string
	Status int
	Result interface{}
	Body   interface{}
}

var (
	client = &http.Client{Timeout: time.Second}
)

func PrepareTestApis(db *sql.DB) {
	qs := []string{
		`DROP TABLE IF EXISTS items;`,

		`CREATE TABLE items (
  id serial PRIMARY KEY,
  title varchar(255) NOT NULL,
  description text NOT NULL,
  updated varchar(255) DEFAULT NULL
);`,

		`INSERT INTO items (title, description, updated) VALUES
('database/sql', 'Рассказать про базы данных', 'rvasily'),
('memcache', 'Рассказать про мемкеш с примером использования', NULL);`,

		`DROP TABLE IF EXISTS users;`,

		`CREATE TABLE users (
			user_id serial PRIMARY KEY,
			login varchar(255) NOT NULL,
			password varchar(255) NOT NULL,
			email varchar(255) NOT NULL,
			info text NOT NULL,
			updated varchar(255) DEFAULT NULL
		);`,

		`INSERT INTO users (login, password, email, info, updated) VALUES
('rvasily', 'love', 'rvasily@example.com', 'none', NULL);`,
	}

	for _, q := range qs {
		_, err := db.Exec(q)
		if err != nil {
			panic(err)
		}
	}
}

func CleanupTestApis(db *sql.DB) {
	qs := []string{
		`DROP TABLE IF EXISTS items;`,
		`DROP TABLE IF EXISTS users;`,
	}
	for _, q := range qs {
		_, err := db.Exec(q)
		if err != nil {
			panic(err)
		}
	}
}

func TestApis(t *testing.T) {
	db, err := sql.Open("postgres", "user=postgres password=********* dbname=db_go sslmode=disable")
	if err != nil {
		t.Fatalf("error opening database: %v", err)
	}

	err = db.Ping()
	if err != nil {
		t.Fatalf("error pinging database: %v", err)
	}

	PrepareTestApis(db)

	defer CleanupTestApis(db)

	handler, err := NewDbExplorer(db)
	if err != nil {
		t.Fatalf("error initializing handler: %v", err)
	}

	ts := httptest.NewServer(handler)

	cases := []Case{
		Case{
			Path: "/",
			Result: CR{
				"response": CR{
					"tables": []string{"items", "users"},
				},
			},
		},
		Case{
			Path:   "/unknown_table",
			Status: http.StatusNotFound,
			Result: CR{
				"error": "unknown table",
			},
		},
		Case{
			Path: "/items",
			Result: CR{
				"response": CR{
					"records": []CR{
						CR{
							"id":          1,
							"title":       "database/sql",
							"description": "Рассказать про базы данных",
							"updated":     "rvasily",
						},
						CR{
							"id":          2,
							"title":       "memcache",
							"description": "Рассказать про мемкеш с примером использования",
							"updated":     nil,
						},
					},
				},
			},
		},
		Case{
			Path: "/users",
			Result: CR{
				"response": CR{
					"records": []CR{
						CR{
							"user_id": 1,
							"login":   "rvasily",
							"password": "love",
							"email":    "rvasily@example.com",
							"info":     "none",
							"updated":  nil,
						},
					},
				},
			},
		},
		Case{
			Path:  "/items",
			Query: "limit=1&offset=1",
			Result: CR{
				"response": CR{
					"records": []CR{
						CR{
							"id":          2,
							"title":       "memcache",
							"description": "Рассказать про мемкеш с примером использования",
							"updated":     nil,
						},
					},
				},
			},
		},
		Case{
			Path: "/items/1",
			Result: CR{
				"response": CR{
					"record": CR{
						"id":          1,
						"title":       "database/sql",
						"description": "Рассказать про базы данных",
						"updated":     "rvasily",
					},
				},
			},
		},
		Case{
			Path:   "/items/100500",
			Status: http.StatusNotFound,
			Result: CR{
				"error": "record not found",
			},
		},
	}

	runCases(t, ts, db, cases)
}


func runCases(t *testing.T, ts *httptest.Server, db *sql.DB, cases []Case) {
	for idx, item := range cases {
		var (
			err      error
			result   interface{}
			expected interface{}
			req      *http.Request
		)

		caseName := fmt.Sprintf("case %d: [%s] %s %s", idx, item.Method, item.Path, item.Query)

		if item.Method == "" || item.Method == http.MethodGet {
			req, err = http.NewRequest(item.Method, ts.URL+item.Path+"?"+item.Query, nil)
		} else {
			data, err := json.Marshal(item.Body)
			if err != nil {
				t.Fatalf("[%s] json marshal error: %v", caseName, err)
			}
			reqBody := bytes.NewReader(data)
			req, err = http.NewRequest(item.Method, ts.URL+item.Path, reqBody)
			req.Header.Add("Content-Type", "application/json")
		}

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("[%s] request error: %v", caseName, err)
			continue
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)

		if item.Status == 0 {
			item.Status = http.StatusOK
		}

		if resp.StatusCode != item.Status {
			t.Fatalf("[%s] expected http status %v, got %v", caseName, item.Status, resp.StatusCode)
			continue
		}

		err = json.Unmarshal(body, &result)
		if err != nil {
			t.Fatalf("[%s] can't unpack json: %v", caseName, err)
			continue
		}

		data, err := json.Marshal(item.Result)
		json.Unmarshal(data, &expected)

		if !reflect.DeepEqual(result, expected) {
			t.Fatalf("[%s] results not match\nGot : %#v\nWant: %#v", caseName, result, expected)
			continue
		}
	}

}
