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

	"github.com/allenkallz/snowflake-provider/apis/database/v1alpha1"
)

type DbInfo struct {
	DbName string
	Kind   string
}

func (c ClientInfo) ListDatabase(ctx context.Context, dbinfo DbInfo) {}

func (c ClientInfo) FetchDatabase(ctx context.Context, dbinfo DbInfo) {}

// create database
func (c ClientInfo) CreateDatabase(ctx context.Context, db *v1alpha1.DatabaseParameters) (string, error) {

	body := DbInfo{DbName: db.Name, Kind: "PERMANENT"}

	// queryParam
	queryParams := url.Values{}
	queryParams.Add("createMode", "errorIfExists")

	fullPath, err := url.JoinPath("https://", c.SnowflakeAccount, "snowflakecomputing.com", "api/v2/databases")
	fullUrl := fmt.Sprintf("%s?%s", fullPath, queryParams.Encode())

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("POST", fullUrl, bytes.NewBuffer(jsonBody))
	if err != nil {
		return "", err
	}

	authToken := fmt.Sprintf("%s %s", "Bearer", c.JwtToken)

	// Add headers to request
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", authToken)

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

func (c ClientInfo) DeleteDatabase(ctx context.Context, dbName string) error {

	// queryParam
	queryParams := url.Values{}
	// handle if dont exist. false will raise error
	queryParams.Add("ifExists", "false")
	// dont delete if forign key exist and return warning
	queryParams.Add("restrict", "true")

	authToken := fmt.Sprintf("%s %s", "Bearer", c.JwtToken)

	fullPath, err := url.JoinPath("https://", c.SnowflakeAccount, "snowflakecomputing.com", "api/v2/databases", dbName)
	fullUrl := fmt.Sprintf("%s?%s", fullPath, queryParams.Encode())

	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", fullUrl, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", authToken)

	// make request
	resp, err := c.httpClient.Do(req)

	defer resp.Body.Close()

	if err != nil {
		fmt.Printf("Failed to delete resource. Status Code: %d\n", resp.StatusCode)
		fmt.Println("Error making request:", err)
	}

	return err

}

func (c ClientInfo) UpdateDatabase(ctx context.Context, dbinfo DbInfo) {}
