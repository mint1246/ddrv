package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/alecthomas/kong"
	"github.com/joho/godotenv"
	zl "github.com/rs/zerolog"

	"github.com/rs/zerolog/log"

	"github.com/forscht/ddrv/internal/config"
	dp "github.com/forscht/ddrv/internal/dataprovider"
	"github.com/forscht/ddrv/internal/dataprovider/bolt"
	"github.com/forscht/ddrv/internal/filesystem"
	"github.com/forscht/ddrv/internal/ftp"
	"github.com/forscht/ddrv/internal/http"
	"github.com/forscht/ddrv/pkg/ddrv"
)

func main() {
	// Set the maximum number of operating system threads to use.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Load .env and override it on os.env
	_ = godotenv.Load()
	env, err := godotenv.Read()
	if err == nil {
		for key, value := range env {
			_ = os.Setenv(key, value)
		}
	}

	// Parse command line arguments into config
	kong.Parse(config.New(), kong.Vars{
		"version": fmt.Sprintf("ddrv %s", version),
	})

	// Setup logger
	log.Logger = zl.New(zl.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	zl.SetGlobalLevel(zl.InfoLevel)
	if config.Debug() {
		zl.SetGlobalLevel(zl.DebugLevel)
	}
	// Create a ddrv manager
	driver, err := ddrv.New(config.Tokens(), config.Channels(), config.ChunkSize())
	if err != nil {
		log.Fatal().Err(err).Str("c", "main").Msg("failed to open ddrv driver")
	}

	// Init dataprovider
	bprovider := bolt.New("./ddrv.db", driver)
	// Load data provider
	dp.Load(bprovider)

	errCh := make(chan error)

	// Create and start ftp server
	if config.FTPAddr() != "" {
		go func() {
			fs := filesystem.New(driver)
			ftpServer := ftp.New(fs, config.FTPAddr())
			log.Info().Str("c", "main").Str("addr", config.FTPAddr()).Msg("starting ftp server")
			errCh <- ftpServer.ListenAndServe()
		}()
	}
	if config.HTTPAddr() != "" {
		go func() {
			httpServer := http.New(driver)
			log.Info().Str("c", "main").Str("addr", config.HTTPAddr()).Msg("starting http server")
			errCh <- httpServer.Listen(config.HTTPAddr())
		}()
	}
	if config.FTPAddr() == "" && config.HTTPAddr() == "" {
		go func() {
			fs := filesystem.New(driver)
			ftpServer := ftp.New(fs, ":2525")
			log.Info().Str("c", "main").Msg("--http-addr and --ftp-addr both missing, ftp server will start on default addr")
			log.Info().Str("c", "main").Str("addr", ":2525").Msg("starting ftp server")
			errCh <- ftpServer.ListenAndServe()
		}()
	}
	log.Fatal().Msgf("ddrv: error %v", <-errCh)
}
