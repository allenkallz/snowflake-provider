package snowflake

import (
	"context"
	"encoding/json"
	"net/http"

	dbv1alpha1 "github.com/allenkallz/provider-snowflake/apis/database/v1alpha1"

	"github.com/allenkallz/provider-snowflake/apis/v1alpha1"
	"github.com/crossplane/crossplane-runtime/pkg/errors"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	invalidName   = "Invalid name"
	requestFailed = "Failed to create request"
	configNotJson = "Spec Config not actually JSON"
)

var ErrNotFound = errors.New("Not found")

type ClientInfo struct {
	SnowflakeAccount string
	JwtToken         string
	httpClient       *http.Client
}

type Client interface {
	// TableClient
	DatabaseClient
}

type DatabaseClient interface {
	ListDatabase(ctx context.Context, dbinfo DbInfo)
	FetchDatabase(ctx context.Context, dbinfo DbInfo)
	CreateDatabase(ctx context.Context, db *dbv1alpha1.DatabaseParameters) (string, error)
	DeleteDatabase(ctx context.Context, db *dbv1alpha1.DatabaseParameters) error
	UpdateDatabase(ctx context.Context, dbinfo DbInfo)
}

// type TableClient interface {
// 	ListTable(ctx context.Context, tableinfo TableInfo)
// 	FetchTable(ctx context.Context, tableinfo TableInfo)
// 	CreateTable(ctx context.Context, tableinfo TableInfo)
// 	DeleteTable(ctx context.Context, tableinfo TableInfo)
// 	UpdateTable(ctx context.Context, tableinfo TableInfo)
// }

func (c *ClientInfo) MakeRequest(method string, api_path string, payload map[string]interface{}) {

}

// func MakeClient(snowflakeaccount string, jwttoken string) ClientInfo {
// 	return ClientInfo{
// 		SnowflakeAccount: snowflakeaccount,
// 		JwtToken:         jwttoken,
// 		httpClient:       &http.Client{},
// 	}
// }

// all helper method
func GetClientInfo(ctx context.Context, c client.Client, mg resource.Managed) (*ClientInfo, error) {

	switch {
	case mg.GetProviderConfigReference() != nil:
		return UseProviderConfig(ctx, c, mg)
	default:
		return nil, errors.New("providerConfigRef is not given")
	}
}

func UseProviderConfig(ctx context.Context, c client.Client, mg resource.Managed) (*ClientInfo, error) {

	pc := &v1alpha1.ProviderConfig{}
	if err := c.Get(ctx, types.NamespacedName{Name: mg.GetProviderConfigReference().Name}, pc); err != nil {
		return nil, errors.Wrap(err, "cannot get referenced Provider")
	}

	t := resource.NewProviderConfigUsageTracker(c, &v1alpha1.ProviderConfigUsage{})
	if err := t.Track(ctx, mg); err != nil {
		return nil, errors.Wrap(err, "cannot track ProviderConfig usage")
	}

	// read authToken from of secretRef
	authToken, err := authFromCredentials(ctx, c, pc.Spec.Credentials)
	if err != nil {
		return nil, err
	}

	return &ClientInfo{
		SnowflakeAccount: pc.Spec.SnowflakeAccount,
		JwtToken:         authToken,
		httpClient:       &http.Client{},
	}, nil
}

// Read token from secret
func authFromCredentials(ctx context.Context, c client.Client, creds v1alpha1.ProviderCredentials) (string, error) {
	csr := creds.SecretRef
	if csr == nil {
		return "", errors.New("no credentials secret referenced")
	}

	s := &corev1.Secret{}

	if err := c.Get(ctx, types.NamespacedName{Namespace: csr.Namespace, Name: csr.Name}, s); err != nil {
		return "", errors.Wrap(err, "cannot get credentials secret")
	}

	return string(s.Data[csr.Key]), nil
}

func configPrep(config string) (map[string]any, error) {
	ret := map[string]any{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &ret)
		if err != nil {
			return ret, errors.Wrap(err, configNotJson)
		}
	}
	return ret, nil
}
