package drivers

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"log"
	"strings"
)

type DuckDBDriver struct {
	db *sql.DB
}

func getBootQueries(awsAccessKeyID string, awsSecretAccessKey string, awsSessionToken string, awsRegion string) []string {
	bootQueries := []string{
		"INSTALL json;",
		"LOAD json;",
		"INSTALL parquet;",
		"LOAD parquet;",
		"INSTALL httpfs;",
		"LOAD httpfs;",
	}
	if awsAccessKeyID != "" {
		bootQueries = append(bootQueries, "SET s3_access_key_id='"+awsAccessKeyID+"';")
	}
	if awsSecretAccessKey != "" {
		bootQueries = append(bootQueries, "SET s3_secret_access_key='"+awsSecretAccessKey+"';")
	}
	if awsSessionToken != "" {
		bootQueries = append(bootQueries, "SET s3_session_token='"+awsSessionToken+"';")
	}
	if awsRegion != "" {
		bootQueries = append(bootQueries, "SET s3_region='"+awsRegion+"';")
	}
	return bootQueries
}

func initializeDB(awsAccessKeyID string, awsSecretAccessKey string, awsSessionToken string, awsRegion string) (*sql.DB, error) {
	bootQueries := getBootQueries(awsAccessKeyID, awsSecretAccessKey, awsSessionToken, awsRegion)
	connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		for _, qry := range bootQueries {
			_, err := execer.ExecContext(context.Background(), qry, make([]driver.NamedValue, 0))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	return db, nil
}

func NewDuckDBDriver(initialLoadingQuery string, awsAccessKeyID string, awsSecretAccessKey string, awsSessionToken string, awsRegion string) *DuckDBDriver {
	log.Print("initializing duckdb driver with initial loading query: " + initialLoadingQuery)
	db, err := initializeDB(awsAccessKeyID, awsSecretAccessKey, awsSessionToken, awsRegion)
	if err != nil {
		panic(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err)
	}
	// load this parquet file into the table
	_, err = db.Exec(initialLoadingQuery)
	if err != nil {
		panic(fmt.Errorf("failed to load initial data: %w", err))
	}
	return &DuckDBDriver{
		db: db,
	}
}

func (d *DuckDBDriver) ExecuteQuery(entity string, filterFields string, selectFields string) (string, error) {

	var datarow string
	var builder strings.Builder

	wrapQuery := "SELECT row_to_json(df) as datarow FROM (%s) df;"
	innerQuery := formatSQLQuery(entity, filterFields, selectFields)
	query := fmt.Sprintf(wrapQuery, innerQuery)
	rows, err := d.db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&datarow)
		if err != nil {
			log.Printf("failed to scan row: %v", err)
			return "", err
		}
		_, err = builder.WriteString(datarow)
		if err != nil {
			log.Printf("failed to build row: %v", err)
			return "", err
		}
	}
	return builder.String(), nil
}

///curl -X POST -H "Content-Type: application/octet-stream" -d "assets[select:id,product_code][filter:acc=\"acc-AAAAAA\"]" http://localhost:3000/query
