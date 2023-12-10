package http

import (
    "errors"
    "log" // Keep this line
    fzl "github.com/gofiber/contrib/fiberzerolog"
    "github.com/gofiber/fiber/v2"
    "github.com/gofiber/fiber/v2/middleware/cors"
    zlog "github.com/rs/zerolog/log" // Add an alias here
    "github.com/forscht/ddrv/internal/http/api" // Add this line
    "github.com/forscht/ddrv/internal/http/web" // Add this line
    "github.com/forscht/ddrv/pkg/ddrv" // Add this line
    // Other imports
)

func New(driver ddrv.Driver) *fiber.App { // Use the package name as a prefix

    // Initialize fiber app
    app := fiber.New(config())

    // Enable logger
    logger := zlog.With().Str("c", "httpserver").Logger() // Use the alias here
    app.Use(fzl.New(fzl.Config{Logger: &logger}))

    // Enable cors
    app.Use(cors.New())

    // Load Web routes
    web.Load(app) // Use the package name as a prefix

    // Register API routes
    api.Load(app, &driver) // Use the package name as a prefix

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
                return ctx.Status(code).JSON(api.Response{Message: err.Error()}) // Use the package name as a prefix
            }
            return ctx.Status(code).JSON(api.Response{Message: "internal server error"}) // Use the package name as a prefix
        },
    }
}

// Start starts the HTTP server
// Start the HTTPS server
func Start(app *fiber.App) {
    // Start the HTTPS server
    log.Println("Starting HTTPS server on port 443")
    // Change the arguments to use the relative path of the certificates
    err := app.ListenTLS(":443", "/etc/sslcertificates/fullchain.pem", "/etc/sslcertificates/privkey.pem")
    if err != nil {
        log.Fatal(err.Error()) // Change this line
    }
}