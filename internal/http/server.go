package http

import (
	"errors"

	fzl "github.com/gofiber/contrib/fiberzerolog"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/rs/zerolog/log"

	"github.com/forscht/ddrv/internal/http/api"
	"github.com/forscht/ddrv/internal/http/web"
	"github.com/forscht/ddrv/pkg/ddrv"
)

func New(driver *ddrv.Driver) *fiber.App {

	// Initialize fiber app
	app := fiber.New(config())

	// Enable logger
	logger := log.With().Str("c", "httpserver").Logger()
	app.Use(fzl.New(fzl.Config{Logger: &logger}))

	// Enable cors
	app.Use(cors.New())

	// Load Web routes
	web.Load(app)

	// Register API routes
	api.Load(app, driver)

	return app
}

func config() fiber.Config {
	//engine := html.New("./http/web/views", ".html")
	return fiber.Config{
		DisablePreParseMultipartForm: true, // https://github.com/gofiber/fiber/issues/1838
		StreamRequestBody:            true,
		DisableStartupMessage:        true,
		ErrorHandler: func(ctx *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError // Status code defaults to 500
			if ctx.BaseURL() == "http://" || ctx.BaseURL() == "https://" {
				return nil
			}
			// Retrieve the custom status code if it's a *fiber.Error
			var e *fiber.Error
			if errors.As(err, &e) {
				code = e.Code
			}
			if code != fiber.StatusInternalServerError {
				return ctx.Status(code).JSON(api.Response{Message: err.Error()})
			}
			return ctx.Status(code).JSON(api.Response{Message: "internal server error"})
		},
	}
}
