package ddrv

import (
	"log"
	"net/url"
	"strconv"
)

// decode parses the input URL and extracts the query parameters.
// It returns the cleaned URL, `ex` and `is` as integers, `hm` as a string, and an error if any.
func decodeAttachmentURL(inputURL string) (string, int, int, string) {
	parsedURL, err := url.Parse(inputURL)
	if err != nil {
		log.Fatalf("decodeAttachmentURL : failed to parse attachmentURL : URL -> %s", inputURL)
	}

	// Extract query parameters
	queryParams := parsedURL.Query()

	// Convert base16 (hexadecimal) values to int
	ex64, err := strconv.ParseInt(queryParams.Get("ex"), 16, 32)
	if err != nil {
		log.Fatalf("failed to convert ex to int : ex -> %s", queryParams.Get("ex"))
	}
	ex := int(ex64)

	is64, err := strconv.ParseInt(queryParams.Get("is"), 16, 32)
	if err != nil {
		log.Fatalf("failed to convert ex to int : is -> %s", queryParams.Get("is"))
	}
	is := int(is64)

	// Extract `hm` as a string
	hm := queryParams.Get("hm")

	// Clean URL
	cleanedURL := parsedURL.Scheme + "://" + parsedURL.Host + parsedURL.Path

	return cleanedURL, ex, is, hm
}

// encode takes a base URL, `ex`, `is`, and `hm` as inputs, and returns the modified URL and the `channelId`.
func encodeAttachmentURL(baseURL string, ex int, is int, hm string) string {
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		log.Fatalf("encodeAttachmentURL : failed to parse attachmentURL : URL -> %s", baseURL)
	}

	// Convert int values to base16 (hexadecimal)
	exHex := strconv.FormatInt(int64(ex), 16)
	isHex := strconv.FormatInt(int64(is), 16)

	// Set query parameters
	queryParams := url.Values{}
	queryParams.Set("ex", exHex)
	queryParams.Set("is", isHex)
	queryParams.Set("hm", hm)

	// Construct the encoded URL
	parsedURL.RawQuery = queryParams.Encode()
	encodedURL := parsedURL.String()

	return encodedURL
}
