package handlers

import (
	"github.com/a-agmon/ssql/drivers"
	"github.com/a-agmon/ssql/processors"
	"github.com/gofiber/fiber/v2"
	"net/http"
)

type FiberHandler struct {
	driver         drivers.Driver
	queryProcessor *processors.QueryProcessor
}

func NewFiberTestHandler(d drivers.Driver) *FiberHandler {
	return &FiberHandler{
		driver:         d,
		queryProcessor: processors.NewQueryProcessor(),
	}
}

func (h *FiberHandler) Handle(c *fiber.Ctx) error {
	payloadString := string(c.Body())
	query, err := h.queryProcessor.Process(payloadString)
	if err != nil {
		return c.Status(http.StatusBadRequest).SendString(err.Error())
	}
	response, err := h.driver.ExecuteQuery(query.Entity, query.Select, query.Filter)
	if err != nil {
		return c.Status(http.StatusInternalServerError).SendString(err.Error())
	}
	c.SendString(response)
	return c.SendStatus(200)
}
