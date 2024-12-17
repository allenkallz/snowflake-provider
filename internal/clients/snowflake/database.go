package snowflake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/pkg/errors"

	"github.com/allenkallz/provider-snowflake/apis/database/v1alpha1"
)

type DbInfo struct {
	name string
	kind string
}

func (c ClientInfo) ListDatabase(ctx context.Context, dbinfo DbInfo) {}

func (c ClientInfo) FetchDatabase(ctx context.Context, db *v1alpha1.DatabaseParameters) (DbInfo, error) {

	// Get token first
	authToken, err := generateJWT(c)
	if err != nil {
		fmt.Println("Error gettingToken:", err)
		return DbInfo{}, err
	}

	fullPath, err := url.JoinPath(getBaseUrl(c), "api/v2/databases", db.Name)

	if err != nil {
		return DbInfo{}, err
	}

	req, err := http.NewRequest("GET", fullPath, nil)
	if err != nil {
		return DbInfo{}, err
	}

	setReqHeaders(req, authToken)

	// make request
	resp, err := c.httpClient.Do(req)

	defer resp.Body.Close()

	if int(resp.StatusCode) >= 400 {
		fmt.Printf("Failed to delete resource. Status Code: %d\n", resp.StatusCode)
		fmt.Println("Error making request:", err)

		return DbInfo{}, nil
	}

	respBody, err := io.ReadAll(resp.Body)

	// Create a map to hold the JSON data
	var jsonResponse DbInfo
	err = json.Unmarshal(respBody, &jsonResponse)

	return DbInfo{
		name: jsonResponse.name,
		kind: jsonResponse.kind,
	}, nil
}

// create database
func (c ClientInfo) CreateDatabase(ctx context.Context, db *v1alpha1.DatabaseParameters) (string, error) {

	// Get token first
	authToken, err := generateJWT(c)
	if err != nil {
		fmt.Println("Error gettingToken:", err)
		return "", err
	}

	body := DbInfo{name: db.Name, kind: "PERMANENT"}

	// queryParam
	queryParams := url.Values{}
	queryParams.Add("createMode", "errorIfExists")

	fullPath, err := url.JoinPath(getBaseUrl(c), "api/v2/databases")
	fullUrl := fmt.Sprintf("%s?%s", fullPath, queryParams.Encode())

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("POST", fullUrl, bytes.NewBuffer(jsonBody))
	if err != nil {
		return "", err
	}

	setReqHeaders(req, authToken)

	// make request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		fmt.Println("Error making request:", err)
		return "", err
	}

	defer resp.Body.Close()

	// Read the response
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return "", err
	}

	// Print the response
	fmt.Println("Response Status:", resp.Status)
	fmt.Println("Response Body:", string(respBody))

	if int(resp.StatusCode) >= 400 {
		return "", errors.Wrap(err, requestFailed)
	}
	return string(resp.StatusCode), err
}

func (c ClientInfo) DeleteDatabase(ctx context.Context, db *v1alpha1.DatabaseParameters) error {

	// Get token first
	authToken, err := generateJWT(c)
	if err != nil {
		fmt.Println("Error gettingToken:", err)
		return err
	}

	// queryParam
	queryParams := url.Values{}
	// handle if dont exist. false will raise error
	queryParams.Add("ifExists", "false")
	// dont delete if forign key exist and return warning
	queryParams.Add("restrict", "true")

	fullPath, err := url.JoinPath(getBaseUrl(c), "api/v2/databases", db.Name)
	fullUrl := fmt.Sprintf("%s?%s", fullPath, queryParams.Encode())

	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", fullUrl, nil)
	if err != nil {
		return err
	}

	setReqHeaders(req, authToken)

	// make request
	resp, err := c.httpClient.Do(req)

	defer resp.Body.Close()

	if int(resp.StatusCode) >= 400 {
		fmt.Printf("Failed to delete resource. Status Code: %d\n", resp.StatusCode)
		fmt.Println("Error making request:", err)
	}

	return err

}

func (c ClientInfo) UpdateDatabase(ctx context.Context, dbinfo DbInfo) {}
