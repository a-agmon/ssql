package main

import (
	"fmt"
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

	dbHost := os.Getenv("POSTGRES_HOST")
	userPass := os.Getenv("POSTGRES_USERPASS")
	postgresConnectionStr := fmt.Sprintf("postgres://%s@%s:5432/postgres?sslmode=disable", userPass, dbHost)

	postgresDriver := drivers.NewPostgresDriver(postgresConnectionStr)
	
	app.Post("/query", handlers.NewFiberTestHandler(postgresDriver).Handle)

	err := app.Listen(":3000")
	if err != nil {
		log.Fatal(err)
	}
}
