package main

import (
	"fmt"
	"log"
	"runtime"

	"github.com/alecthomas/kong"
	"github.com/joho/godotenv"

	"github.com/forscht/ddrv/internal/config"
	dp "github.com/forscht/ddrv/internal/dataprovider"
	"github.com/forscht/ddrv/internal/dataprovider/pgsql"
	"github.com/forscht/ddrv/internal/filesystem"
	"github.com/forscht/ddrv/internal/ftp"
	"github.com/forscht/ddrv/internal/http"
	"github.com/forscht/ddrv/pkg/ddrv"
)

func main() {
	// Set the maximum number of operating system threads to use.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Load env file.
	_ = godotenv.Load()

	// Parse command line arguments into config
	kong.Parse(config.New(), kong.Vars{
		"version": fmt.Sprintf("ddrv %s", version),
	})

	// Create a ddrv manager
	driver, err := ddrv.New(config.Tokens(), config.Channels(), config.ChunkSize())
	if err != nil {
		log.Fatalf("ddrv: failed to open ddrv driver :%v", err)
	}

	// Create FS object
	fs := filesystem.New(driver)

	// Load data provider
	dp.Load(pgsql.New(config.DbURL(), driver))

	errCh := make(chan error)

	// Create and start ftp server
	if config.FTPAddr() != "" {
		go func() {
			ftpServer := ftp.New(fs)
			log.Printf("ddrv: starting FTP server on : %s", config.FTPAddr())
			errCh <- ftpServer.ListenAndServe()
		}()
	}
	if config.HTTPAddr() != "" {
		go func() {
			httpServer := http.New(driver)
			log.Printf("ddrv: starting HTTP server on : %s", config.HTTPAddr())
			errCh <- httpServer.Listen(config.HTTPAddr())
		}()
	}

	log.Fatalf("ddrv: ddrv error %v", <-errCh)
}
