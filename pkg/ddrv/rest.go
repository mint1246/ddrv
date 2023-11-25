package ddrv

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

var baseURL = "https://discord.com/api/v9"
var UserAgent = "PostmanRuntime/7.35.0"

type Rest struct {
	nitro        bool
	channels     []string
	lastChIdx    int
	limiter      *Limiter
	client       *http.Client
	tokens       []string
	mu           *sync.Mutex
	lastTokenIdx int
}

func NewRest(tokens []string, channels []string, nitro bool) *Rest {
	return &Rest{
		client:       &http.Client{Timeout: 30 * time.Second},
		nitro:        nitro,
		channels:     channels,
		limiter:      NewLimiter(),
		tokens:       tokens,
		mu:           &sync.Mutex{},
		lastTokenIdx: 0,
		lastChIdx:    0,
	}
}

func (r *Rest) request(method string, path string, token string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, baseURL+path, body)
	if err != nil {
		return nil, err
	}

	req.Header.Add("User-Agent", UserAgent)
	if token != "" {
		req.Header.Add("Authorization", token)
	}

	return req, nil
}

// token returns the next token in the list, cycling through the list in a round-robin manner.
func (r *Rest) token() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Select the next token
	token := r.tokens[r.lastTokenIdx]
	// Update the index of the last used token, wrapping around to the start of the list if necessary
	r.lastTokenIdx = (r.lastTokenIdx + 1) % len(r.tokens)

	return token
}

// channel returns the next channel in the list, cycling through the list in a round-robin manner.
func (r *Rest) channel() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Select the next channel
	channel := r.channels[r.lastChIdx]
	// Update the index of the last used channel, wrapping around to the start of the list if necessary
	r.lastChIdx = (r.lastChIdx + 1) % len(r.channels)

	return channel
}

func (r *Rest) GetMessages(channelId string, messageId string, query string) (*[]Message, error) {
	token := r.token()
	var path string
	if messageId != "" && query != "" {
		path = fmt.Sprintf("/channels/%s/messages?limit=100&%s=%s", channelId, query, messageId)
	} else {
		path = fmt.Sprintf("/channels/%s/messages?limit=100", channelId)
	}
	bucketPath := fmt.Sprintf("%s/channels/%s/messages", token, channelId)

	// Create request
	req, err := r.request(http.MethodGet, path, token, nil)
	if err != nil {
		return nil, err
	}

	// Try to acquire lock
	if err := r.limiter.Acquire(bucketPath); err != nil {
		return nil, err
	}

	// Here make HTTP call
	resp, err := r.client.Do(req)
	// Release lock
	if resp != nil && resp.Header != nil {
		r.limiter.Release(bucketPath, resp.Header)
	}
	if err != nil {
		r.limiter.Release(bucketPath, nil)
		return nil, err
	}

	// Retry request on 429 or >500
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError {
		return r.GetMessages(channelId, messageId, query)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rest getmessages: expected status code %d - received %d", http.StatusOK, resp.StatusCode)
	}
	// read and parse the response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var m []Message
	if err := json.Unmarshal(respBody, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// CreateAttachment uploads a file to the Discord channel using the webhook.
func (r *Rest) CreateAttachment(reader io.Reader) (*Chunk, error) {
	token := r.token()
	channelId := r.channel()
	path := fmt.Sprintf("/channels/%s/messages", channelId)
	bucketPath := fmt.Sprintf("%s/channels/%s/messages", token, channelId)

	// Prepare request
	contentType, body := mbody(reader)
	req, err := r.request(http.MethodPost, path, token, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", contentType)
	// Try to acquire lock
	if err := r.limiter.Acquire(bucketPath); err != nil {
		return nil, err
	}

	// Here make HTTP call
	resp, err := r.client.Do(req)
	// Release lock
	if resp != nil && resp.Header != nil {
		r.limiter.Release(bucketPath, resp.Header)
	}
	if err != nil {
		r.limiter.Release(bucketPath, nil)
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("create attachment : expected status code %d but recevied %d", http.StatusOK, resp.StatusCode)
	}
	// read and parse the response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var m Message
	if err := json.Unmarshal(respBody, &m); err != nil {
		return nil, err
	}
	// clean url and extract ex,is and hm
	att := m.Attachments[0]
	att.URL, att.Ex, att.Is, att.Hm = decodeAttachmentURL(att.URL)
	// Return the first attachment from the response
	return &att, nil
}

func (r *Rest) ReadAttachment(att *Chunk, start int, end int) (io.ReadCloser, error) {
	path := encodeAttachmentURL(att.URL, att.Ex, att.Is, att.Hm)
	req, err := r.request(http.MethodGet, path, "", nil)
	if err != nil {
		return nil, err
	}
	// Set the Range header to specify the range of data to fetch
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	res, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode > http.StatusInternalServerError {
		return r.ReadAttachment(att, start, end)
	}
	if res.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("read attachment : expected code %d but received %d", http.StatusPartialContent, res.StatusCode)
	}
	// Return the body of the response, which contains the requested data
	return res.Body, nil

}

// mbody creates the multipart form-data body to upload a file to the Discord channel using the webhook.
func mbody(reader io.Reader) (string, io.Reader) {
	boundary := "disgosucks"
	// Set the content type including the boundary
	contentType := fmt.Sprintf("multipart/form-data; boundary=%s", boundary)

	CRLF := "\r\n"
	fname := uuid.New().String()

	// Assemble all the parts of the multipart form-data
	parts := []io.Reader{
		strings.NewReader("--" + boundary + CRLF),
		strings.NewReader(fmt.Sprintf(`Content-Disposition: form-data; name="%s"; filename="%s"`, fname, fname) + CRLF),
		strings.NewReader(fmt.Sprintf(`Content-Type: %s`, "application/octet-stream") + CRLF),
		strings.NewReader(CRLF),
		reader,
		strings.NewReader(CRLF),
		strings.NewReader("--" + boundary + "--" + CRLF),
	}

	// Return the content type and the combined reader of all parts
	return contentType, io.MultiReader(parts...)
}
