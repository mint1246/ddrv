package api

import (
	"github.com/gofiber/fiber/v2"

	"github.com/forscht/ddrv/internal/config"
	"github.com/forscht/ddrv/pkg/ddrv"
	"github.com/forscht/ddrv/pkg/validator"
)

var validate = validator.New()

func Load(app *fiber.App, driver *ddrv.Driver) {

	// create api API group
	api := app.Group("/api")

	// public route for public login
	api.Post("/user/login", LoginHandler())
	// returns necessary ddrv auth config
	api.Get("/config", AuthConfigHandler())

	// setup auth middleware if username and password are not blank
	if config.Username() != "" || config.Password() != "" {
		api.Use(AuthHandler())
	}

	// verify JWT token (required on a page load)
	api.Get("/check_token", CheckTokenHandler())

	// Load directory middlewares
	api.Post("/directories/", CreateDirHandler())
	api.Get("/directories/:id<guid>?", GetDirHandler())
	api.Put("/directories/:id<guid>", UpdateDirHandler())
	api.Delete("/directories/:id<guid>", DelDirHandler())

	// Load file middlewares
	api.Post("/directories/:dirId<guid>/files", CreateFileHandler(driver))
	api.Get("/directories/:dirId<guid>/files/:id<guid>", GetFileHandler())
	api.Put("/directories/:dirId<guid>/files/:id<guid>", UpdateFileHandler())
	api.Delete("/directories/:dirId<guid>/files/:id<guid>", DelFileHandler())

	// Just like discord, we will not authorize file endpoints
	// so that it can work with download managers or media players
	app.Get("/files/:id", DownloadFileHandler(driver))
	app.Get("/files/:id/:fname", DownloadFileHandler(driver))
}
