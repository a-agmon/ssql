package drivers

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"strings"
)

type PostgresDriver struct {
	connectionString string
}

func NewPostgresDriver(connectionString string) *PostgresDriver {
	return &PostgresDriver{connectionString: connectionString}
}

func (d *PostgresDriver) ExecuteQuery(entity string, filterFields string, selectFields string) (string, error) {
	dbPool, err := pgxpool.New(context.Background(), d.connectionString)
	if err != nil {
		return "", err
	}
	defer dbPool.Close()
	query := formatSQLQuery(entity, filterFields, selectFields)
	log.Printf("Executing query: %s", query)
	rows, err := dbPool.Query(context.Background(), query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	results, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil {
		return "", err
	}
	jsonString, err := json.Marshal(results)
	if err != nil {
		return "", err
	}
	return string(jsonString), nil
}

func formatSQLQuery(entity string, filterFields string, selectFields string) string {
	whereClause := strings.Replace(filterFields, ",", " AND ", -1)
	whereClause = strings.Replace(whereClause, "*", "1=1", -1)
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", selectFields, entity, whereClause)
	query = strings.ReplaceAll(query, "\"", "'")
	return query
}
