package main

import (
	"github.com/a-agmon/ssql/drivers"
	"github.com/a-agmon/ssql/handlers"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"log"
	"os"
)

func main() {
	app := fiber.New()
	app.Use(logger.New(logger.Config{
		Format: "[${time}] ${status} - ${latency} ${method} ${path} :: ${body} => ${resBody}\n",
	}))

	//dbHost := os.Getenv("POSTGRES_HOST")
	//userPass := os.Getenv("POSTGRES_USERPASS")
	//postgresConnectionStr := fmt.Sprintf("postgres://%s@%s:5432/postgres?sslmode=disable", userPass, dbHost)
	//postgresDriver := drivers.NewPostgresDriver(postgresConnectionStr)

	// format this string
	// CREATE TABLE assets AS
	// select id, product_code, acc, product_name, asset_type
	// from parquet_scan('s3://af-biz-dev/rds_replication/parquet/salesforce.asset/*.parquet')

	initQuery := "CREATE TABLE assets AS select id, product_code, acc, product_name, asset_type from read_parquet('s3://af-biz-dev/rds_replication/parquet/salesforce.asset/*.parquet')"
	// get the aws credentials from the environment variables
	awsSessionKey := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSessionSecret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsSessionToken := os.Getenv("AWS_SESSION_TOKEN")
	awsRegion := os.Getenv("AWS_REGION")

	duckdbDrv := drivers.NewDuckDBDriver(initQuery, awsSessionKey, awsSessionSecret, awsSessionToken, awsRegion)

	app.Post("/query", handlers.NewFiberTestHandler(duckdbDrv).Handle)

	err := app.Listen(":3000")
	if err != nil {
		log.Fatal(err)
	}
}

// Suppose to answer the following query: Tablename[filterFields][selectFields]
// curl -X POST -H "Content-Type: application/octet-stream" -d "customers[select:name,id,age][filter:country=\"USA\",age=18]" http://localhost:3000/query
// filter:* will add 1=1 on the postgres driver
