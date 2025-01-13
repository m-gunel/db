package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type DbExplorer struct {
	db     *sql.DB
	tables map[string][]string
}

func NewDbExplorer(db *sql.DB) (*DbExplorer, error) {
	explorer := &DbExplorer{db: db}
	if err := explorer.loadTables(); err != nil {
		return nil, err
	}
	return explorer, nil
}

func (de *DbExplorer) loadTables() error {
	de.tables = make(map[string][]string)
	rows, err := de.db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
	if err != nil {
		return err
	}
	defer rows.Close() 

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		de.tables[tableName] = nil
	}
	return nil
}

func (de *DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	if len(parts) == 1 && parts[0] == "" {
		de.handleRoot(w, r)
		return
	}

	tableName := parts[0]
	if _, ok := de.tables[tableName]; !ok {
		http.Error(w, `{"error": "unknown table"}`, http.StatusNotFound)
		return
	}

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			de.handleGetTable(w, r, tableName)
		case http.MethodPut:
			de.handlePutTable(w, r, tableName)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	if len(parts) == 2 {
		id := parts[1]
		switch r.Method {
		case http.MethodGet:
			de.handleGetRecord(w, r, tableName, id)
		case http.MethodPost:
			de.handlePostRecord(w, r, tableName, id)
		case http.MethodDelete:
			de.handleDeleteRecord(w, r, tableName, id)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func (de *DbExplorer) handleRoot(w http.ResponseWriter, r *http.Request) {
	tables := make([]string, 0, len(de.tables))
	for tableName := range de.tables {
		tables = append(tables, tableName)
	}

	sort.Strings(tables)

	response := map[string]interface{}{
		"response": map[string]interface{}{
			"tables": tables,
		},
	}
	json.NewEncoder(w).Encode(response)
}

func (de *DbExplorer) handleGetTable(w http.ResponseWriter, r *http.Request, tableName string) {
	limit := r.URL.Query().Get("limit")
	offset := r.URL.Query().Get("offset")

	limitValue := "100"
	offsetValue := "0"
	if limit != "" {
		limitValue = limit
	}
	if offset != "" {
		offsetValue = offset
	}

	query := fmt.Sprintf("SELECT * FROM %s LIMIT %s OFFSET %s", tableName, limitValue, offsetValue)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := de.db.QueryContext(ctx, query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var result []map[string]interface{}
	for rows.Next() {
		columnPointers := make([]interface{}, len(columns))
		for i := range columnPointers {
			columnPointers[i] = new(interface{})
		}

		if err := rows.Scan(columnPointers...); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		rowMap := make(map[string]interface{})
		for i, colName := range columns {
			val := *(columnPointers[i].(*interface{}))
			rowMap[colName] = val
		}
		result = append(result, rowMap)
	}

	response := map[string]interface{}{
		"response": map[string]interface{}{
			"records": result,
		},
	}
	json.NewEncoder(w).Encode(response)
}

func (de *DbExplorer) handlePutTable(w http.ResponseWriter, r *http.Request, tableName string) {
	var data map[string]interface{}
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&data); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	id, ok := data["id"]
	if !ok {
		http.Error(w, "Missing 'id' field", http.StatusBadRequest)
		return
	}

	setClauses := []string{}
	values := []interface{}{}
	for key, value := range data {
		if key == "id" {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", key, len(values)+1))
		values = append(values, value)
	}
	values = append(values, id)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = $%d", tableName, strings.Join(setClauses, ", "), len(values))

	_, err := de.db.Exec(query, values...)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error updating record: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"response": map[string]interface{}{
			"id": id,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}


func (de *DbExplorer) handleGetRecord(w http.ResponseWriter, r *http.Request, tableName, id string) {
	row := de.db.QueryRow("SELECT * FROM "+tableName+" WHERE id = $1", id)

	columns, err := de.db.Query("SELECT column_name FROM information_schema.columns WHERE table_name = $1", tableName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer columns.Close()

	var columnNames []string
	for columns.Next() {
		var columnName string
		if err := columns.Scan(&columnName); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		columnNames = append(columnNames, columnName)
	}

	columnPointers := make([]interface{}, len(columnNames))
	for i := range columnPointers {
		columnPointers[i] = new(interface{})
	}

	if err := row.Scan(columnPointers...); err == sql.ErrNoRows {
		http.Error(w, `{"error": "record not found"}`, http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rowMap := make(map[string]interface{})
	for i, colName := range columnNames {
		val := *(columnPointers[i].(*interface{}))
		rowMap[colName] = val
	}

	response := map[string]interface{}{
		"response": map[string]interface{}{
			"record": rowMap,
		},
	}
	json.NewEncoder(w).Encode(response)
}


func (de *DbExplorer) handlePostRecord(w http.ResponseWriter, r *http.Request, tableName, id string) {
	http.Error(w, "POST operation not implemented for this table", http.StatusNotImplemented)
}

func (de *DbExplorer) handleDeleteRecord(w http.ResponseWriter, r *http.Request, tableName, id string) {
	http.Error(w, "DELETE operation not implemented for this table", http.StatusNotImplemented)
}

