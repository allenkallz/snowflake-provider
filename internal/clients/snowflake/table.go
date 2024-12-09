package snowflake

import (
	"bytes"
	"context"
	"text/template"
)

type CreateTableBody struct{}

type TableInfo struct {
	Schema    string
	DbName    string
	TableName *string
}

// function create request path from table info
func TableReqPath(tableinfo TableInfo) (string, error) {

	tmpl := "/api/v2/databases/{{.DbName}}/schemas/{{.Schema}}/tables/{{if .TableName}} {{.TableName}}{{end}}"
	// parse the tmpl and populate the db name schema and table
	t, err := template.New("path").Parse(tmpl)

	if err != nil {
		return "", err
	}

	var result bytes.Buffer

	// Execute the template and write the result to the buffer
	err = t.Execute(&result, tableinfo)
	if err != nil {
		return "", err
	}

	// Return the resulting string
	return result.String(), nil

}

// All operation specific to  table

// List table
func (c ClientInfo) ListTable(ctx context.Context, tableinfo TableInfo) {}

// Read one table info
func (c ClientInfo) FetchTable(ctx context.Context, tableinfo TableInfo) {}

// Create new Table
func (c ClientInfo) CreateTable(ctx context.Context, tableinfo TableInfo, table string, columns map[string]interface{}) {
}

// Delete the existing Table
func (c ClientInfo) DeleteTable(ctx context.Context, tableinfo TableInfo) {}

// Update the existing Table
func (c ClientInfo) UpdateTable(ctx context.Context, tableinfo TableInfo, columns map[string]interface{}) {
}
