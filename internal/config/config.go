package config

import (
	"strings"

	"github.com/alecthomas/kong"
)

type Config struct {
	FTPAddr      string           `help:"Network address for the FTP server to bind to. It defaults to ':2525' meaning it listens on all interfaces." env:"FTP_ADDR"`
	FTPPortRange string           `help:"Range of ports to be used for passive FTP connections. The range is provided as a string in the format 'start-end'." env:"FTP_PORT_RANGE"`
	User         string           `help:"Username for the ddrv service, used for FTP, HTTP or WEBDAV access authentication." env:"USER"`
	Password     string           `help:"Password for the ddrv service, used for FTP, HTTP or WEBDAV access authentication." env:"PASSWORD"`
	HTTPAddr     string           `help:"Network address for the HTTP server to bind to" env:"HTTP_ADDR"`
	HTTPGuest    bool             `help:"If true, enables read-only guest access to the HTTP file manager without login." env:"HTTP_GUEST" default:"false"`
	DbURL        string           `help:"Connection string for the Postgres database. The format should be: postgres://user:password@localhost:port/database?sslmode=disable" env:"DATABASE_URL" required:""`
	Tokens       string           `help:"Discord bot tokens separated by ','" env:"TOKENS" required:""`
	Channels     string           `help:"Discord server channels separated by ','" env:"CHANNELS" required:""`
	ChunkSize    int              `help:"The maximum size in bytes of chunks to be sent via Manager webhook. By default, it's set to 24MB (25165824 bytes)." env:"CHUNK_SIZE" default:"25165824"`
	AsyncWrite   bool             `help:"Enables concurrent file uploads to Discord, resulting in faster file transfers. Note that this will use significantly more RAM, approximately (chunkSize * number of webhooks) + 20% extra bytes. Use with caution based on your system's memory capacity." env:"ASYNC_WRITE" default:"false"`
	Debug        bool             `help:"Sets log level to debug" env:"DEBUG" default:"false"`
	Version      kong.VersionFlag `kong:"name='version', help='Display version.'"`
}

var config *Config

// New creates a new configuration object and assigns it to the global variable.
func New() *Config {
	config = new(Config)
	return config
}

func FTPAddr() string      { return config.FTPAddr }
func FTPPortRange() string { return config.FTPPortRange }
func Username() string     { return config.User }
func Password() string     { return config.Password }
func HTTPAddr() string     { return config.HTTPAddr }
func HTTPGuest() bool      { return config.HTTPGuest }
func DbURL() string        { return config.DbURL }
func ChunkSize() int       { return config.ChunkSize }
func AsyncWrite() bool     { return config.AsyncWrite }
func Tokens() []string     { return strings.Split(config.Tokens, ",") }
func Channels() []string   { return strings.Split(config.Channels, ",") }
func Debug() bool          { return config.Debug }
