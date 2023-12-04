# ddrv
**Turn Discord into infinite cloud storage with support for HTTP, WebDAV, and FTP.**

## About
ddrv is a Go application that exploits Discord's feature of unlimited file attachments (up to 25MB per file) to provide virtually infinite cloud storage.
It uses an FTP frontend as well as HTTP and WebDAV, allowing users to upload any size of file. Behind the scene, ddrv cuts the file into 25MB chunks and uploads it to Discord, all the while maintaining the file's metadata in a PostgreSQL database.

## Highlights
- Theoretically infinite file size
- Unlimited storage space.
- Support for multiple protocols including HTTP, WebDAV, and FTP, which ensures compatibility with a variety of devices and operating systems.
- Robust frontend that works with numerous clients such as web browsers, Filezilla, RClone, Windows Explorer, Ubuntu (Nautilus), and many more.
- PostgreSQL-based file metadata management: File metadata is systematically stored and managed in a PostgreSQL database, enabling efficient file access and manipulation.
- HTTP supports partial downloads ([range headers](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range)), so you can use multi-threaded download managers and stream video directly to video players. 

## Prerequisites
- **PostgreSQL** (Data provider - To store file metadata)
- **Discord WebhookURLs** (To store actual file data)

## Getting Started
- Navigate to the [Release page](https://github.com/forscht/ddrv/releases) and download the latest release suitable for your platform.
- Decompress the binary file and initialize ddrv with the command provided below:
    ```shell
    ./ddrv --db-url=postgres://user:pass@host:port/dbname?sslmode=disable --webhooks=webhookURL1,webhookURL2
    ```
- An FTP server will launch on `ftp://localhost:2525`
- An HTTP server will launch on `http://localhost:2526`
- An WebDav server will launch on `webdav://localhost:2527`

## Build from Source
- Ensure that you have [Go](https://go.dev/doc/install) (version 1.20 or newer) installed on your system.
- Clone the repository:
  ```shell
  git clone https://github.com/forscht/ddrv.git
  ```
- Navigate to the cloned directory:
  ```shell
  cd ddrv
  ```
- Build the project:
  ```shell
  go build -race -o ddrv ./cmd/ddrv
  # If you're on linux and make is installed
  # make build
  ```
- Run the executable:
  ```shell
  ./ddrv --db-url=postgres://user:pass@host:port/dbname?sslmode=disable --webhooks=webhookURL1,webhookURL2
  ```

### License
[ddrv](/) is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0). See the [LICENSE](LICENSE) file for full license details. <br /> 
For quick reference, the AGPL-3.0 permits use, duplication, modification, and distribution of the software, given that the conditions of the license are met. This license also includes a requirement that if you run modifications of the software on a network, you must make the modified source code available to users. However, the software is provided "as is" and without any warranty. For more information, please read the full license text.
