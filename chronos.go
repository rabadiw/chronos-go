package chronos

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// Constants to represent HTTP verbs
const (
	HTTPGet    = "GET"
	HTTPPut    = "PUT"
	HTTPDelete = "DELETE"
	HTTPPost   = "POST"
)

// BasicAuth a basic crendential
type BasicAuth struct {
	Username string
	Password string
}

// Chronos chronos HTTP client
type Chronos struct {
	URL            *url.URL
	http           *http.Client
	Debug          bool
	RequestTimeout int
	APIPrefix      string
	BasicAuth      BasicAuth
}

// DefaultChronos default Chronos object
// with endpoint http://127.0.0.1:4400
func DefaultChronos() *Chronos {
	url, _ := url.Parse("http://127.0.0.1:4400")
	return &Chronos{
		URL:            url,
		Debug:          false,
		RequestTimeout: 5,
		APIPrefix:      "",
	}
}

// Init initializes http client and verifies endpoint
func (client *Chronos) Init() (*Chronos, error) {

	client.http = &http.Client{
		Timeout: (time.Duration(client.RequestTimeout) * time.Second),
	}

	if _, err := client.Jobs(); err != nil {
		return client, errors.New("Could not reach chronos cluster: " + err.Error())
	}

	return client, nil
}

func (client *Chronos) apiGet(uri string, queryParams map[string]string, result interface{}) error {
	_, err := client.apiCall(HTTPGet, uri, queryParams, "", result)
	return err
}

func (client *Chronos) apiDelete(uri string, queryParams map[string]string, result interface{}) error {
	_, err := client.apiCall(HTTPDelete, uri, queryParams, "", result)
	return err
}

func (client *Chronos) apiPut(uri string, queryParams map[string]string, result interface{}) error {
	_, err := client.apiCall(HTTPPut, uri, queryParams, "", result)
	return err
}

func (client *Chronos) apiPost(uri string, queryParams map[string]string, postData interface{}, result interface{}) error {
	postDataString, err := json.Marshal(postData)

	if err != nil {
		return err
	}

	_, err = client.apiCall(HTTPPost, uri, queryParams, string(postDataString), result)
	return err
}

func (client *Chronos) apiCall(method string, uri string, queryParams map[string]string, body string, result interface{}) (int, error) {
	client.buildURL(uri, queryParams)
	status, response, err := client.httpCall(method, body)

	if err != nil {
		return 0, err
	}

	if response.ContentLength != 0 {
		err = json.NewDecoder(response.Body).Decode(result)

		if err != nil {
			return status, err
		}
	}

	// TODO: Handle error status codes
	if status < 200 || status > 299 {
		return status, errors.New(response.Status)
	}
	return status, nil
}

func (client *Chronos) buildURL(uri string, queryParams map[string]string) {
	query := client.URL.Query()
	for k, v := range queryParams {
		query.Add(k, v)
	}
	client.URL.RawQuery = query.Encode()

	client.URL.Path = path.Join(client.APIPrefix, uri)
}

// TODO: think about pulling out a Request struct/object/thing
func (client *Chronos) applyRequestHeaders(request *http.Request) {
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")
	request.SetBasicAuth(
		client.BasicAuth.Username,
		client.BasicAuth.Password)
}

func (client *Chronos) newRequest(method string, body string) (*http.Request, error) {
	request, err := http.NewRequest(method, client.URL.String(), strings.NewReader(body))

	if err != nil {
		return nil, err
	}

	client.applyRequestHeaders(request)
	return request, nil
}

func (client *Chronos) httpCall(method string, body string) (int, *http.Response, error) {
	request, err := client.newRequest(method, body)

	if err != nil {
		return 0, nil, err
	}

	response, err := client.http.Do(request)

	if err != nil {
		return 0, nil, err
	}

	return response.StatusCode, response, nil
}

// TODO: this better
func (client *Chronos) log(message string, args ...interface{}) {
	fmt.Printf(message+"\n", args...)
}
