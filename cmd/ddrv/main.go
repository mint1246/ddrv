package main

import (
        "fmt"
        "log"
        "os" // Add this line
        "runtime"
        "strings"

        "github.com/alecthomas/kong"
        "github.com/joho/godotenv"

        "github.com/forscht/ddrv/internal/config"
        "github.com/forscht/ddrv/internal/dataprovider"
        "github.com/forscht/ddrv/internal/filesystem"
        "github.com/forscht/ddrv/internal/ftp"
        "github.com/forscht/ddrv/internal/http"
        "github.com/forscht/ddrv/internal/webdav"
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

        // Make sure chunkSize is below 25MB
        if config.ChunkSize() > 25*1024*1024 || config.ChunkSize() < 0 {
                log.Fatalf("ddrv: invalid chunkSize %d", config.ChunkSize())
        }

        // Create a ddrv manager
        mgr, err := ddrv.NewManager(config.ChunkSize(), strings.Split(config.Webhooks(), ","))
        if err != nil {
                log.Fatalf("ddrv: failed to open ddrv mgr :%v", err)
        }

        // Create FS object
        fs := filesystem.New(mgr)

        // New data provider
        dataprovider.New()

        errCh := make(chan error)

    // Check if the sslcertificates folder exists
    _, err = os.Stat("/etc/sslcertificates")
    if os.IsNotExist(err) {
        // Create the sslcertificates folder with 0755 permission mode
        err = os.Mkdir("/etc/sslcertificates", 0755)
        if err != nil {
            // Handle the error
            log.Fatal("ddrv: failed to create sslcertificates folder: %v", err)
        }
        // Log the success
        log.Println("ddrv: created sslcertificates folder")
    }

	if config.FTPAddr() != "" {
			go func() {
					// Create and start ftp server
					ftpServer := ftp.New(fs)
					log.Printf("ddrv: starting FTP server on : %s", config.FTPAddr())
					errCh <- ftpServer.ListenAndServe()
			}()
	}
	if config.HTTPAddr() != "" {
			go func() {
					httpServer := http.New(mgr)
		// Start the HTTPS server
		http.Start(httpServer)
					log.Printf("ddrv: starting HTTP server on : %s", config.HTTPAddr())
					errCh <- httpServer.Listen(config.HTTPAddr())
			}()
	}

	if config.WDAddr() != "" {
			go func() {
					webdavServer := webdav.New(fs)
					log.Printf("ddrv: starting WEBDAV server on : %s", config.WDAddr())
					errCh <- webdavServer.ListenAndServe()
			}()
	}

	log.Fatalf("ddrv: ddrv error %v", <-errCh)
}
