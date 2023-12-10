package api

import (
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"

	"github.com/forscht/ddrv/internal/config"
)

func LoginHandler() fiber.Handler {
	username := config.Username()
	password := config.Password()

	secretKey := fmt.Sprintf("%s:%s", username, password)
	return func(c *fiber.Ctx) error {
		user := new(User)
		err := c.BodyParser(user)
		if err != nil {
			return fiber.NewError(StatusBadRequest, ErrBadRequest)
		}

		if user.Username != username || user.Password != password {
			return fiber.NewError(StatusUnauthorized, ErrBadUsernamePassword)
		}

		// Create the Claims, just to keep each token unique
		claims := jwt.MapClaims{
			"date": time.Now().Nanosecond(),
		}

		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

		t, err := token.SignedString([]byte(secretKey))
		if err != nil {
			return err
		}

		return c.JSON(Response{Message: "login successful", Data: t})
	}
}

func AuthHandler() fiber.Handler {
	guestAllowed := config.HTTPGuest()
	secretKey := fmt.Sprintf("%s:%s", config.Username(), config.Password())
	return func(c *fiber.Ctx) error {
		// If guests are allowed, enable readonly ops
		if guestAllowed {
			switch c.Method() {
			case fiber.MethodGet, fiber.MethodHead, fiber.MethodOptions:
				return c.Next()
			}
		}
		// Get the authorization header
		authHeader := c.Get("Authorization")
		if authHeader == "" {
			return fiber.NewError(StatusUnauthorized, ErrUnauthorized)
		}

		// Extract the token from the header
		tokenStr := strings.TrimSpace(strings.Replace(authHeader, "Bearer", "", 1))

		// Parse the token
		token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
			if t.Method.Alg() != jwt.SigningMethodHS256.Alg() {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return []byte(secretKey), nil
		})

		if err != nil || !token.Valid {
			return fiber.NewError(StatusUnauthorized, ErrUnauthorized)
		}

		return c.Next()
	}
}

func AuthConfigHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		login := true
		if config.Username() == "" || config.Password() == "" {
			login = false
		}
		response := Response{
			Message: "config retrieved",
			Data: map[string]interface{}{
				"login":     login,
				"anonymous": config.HTTPGuest(),
			},
		}
		return c.Status(StatusOk).JSON(response)
	}
}

func CheckTokenHandler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		return c.Status(StatusOk).JSON(Response{Message: "token ok"})
	}
}
