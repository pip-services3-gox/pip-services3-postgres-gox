package persistence

import (
	"context"
	"github.com/jackc/pgx/v4"
	cconv "github.com/pip-services3-gox/pip-services3-commons-gox/convert"
	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	cmpersist "github.com/pip-services3-gox/pip-services3-data-gox/persistence"
)

// IdentifiableJsonPostgresPersistence is an abstract persistence component that stores data in PostgreSQL in JSON or JSONB fields
// and implements a number of CRUD operations over data items with unique ids.
// The data items must implement IIdentifiable interface.
//
// The JSON table has only two fields: id and data.
//
// In basic scenarios child classes shall only override getPageByFilter,
// getListByFilter or deleteByFilter operations with specific filter function.
// All other operations can be used out of the box.
//
// In complex scenarios child classes can implement additional operations by
// accessing c._collection and c._model properties.
//
//	### Configuration parameters ###
//
//		- collection:                  (optional) PostgreSQL collection name
//		- connection(s):
//			- discovery_key:             (optional) a key to retrieve the connection from IDiscovery
//			- host:                      host name or IP address
//			- port:                      port number (default: 27017)
//			- uri:                       resource URI or connection string with all parameters in it
//		- credential(s):
//			- store_key:                 (optional) a key to retrieve the credentials from ICredentialStore
//			- username:                  (optional) user name
//			- password:                  (optional) user password
//		- options:
//			- connect_timeout:      (optional) number of milliseconds to wait before timing out when connecting a new client (default: 0)
//			- idle_timeout:         (optional) number of milliseconds a client must sit idle in the pool and not be checked out (default: 10000)
//			- max_pool_size:        (optional) maximum number of clients the pool should contain (default: 10)
//
//	### References ###
//		- \*:logger:\*:\*:1.0           (optional) ILogger components to pass log messages components to pass log messages
//		- \*:discovery:\*:\*:1.0        (optional) IDiscovery services
//		- \*:credential-store:\*:\*:1.0 (optional) Credential stores to resolve credentials
//
// ### Example ###
type IdentifiableJsonPostgresPersistence[T any, K any] struct {
	IdentifiablePostgresPersistence[T, K]
}

// InheritIdentifiableJsonPostgresPersistence creates a new instance of the persistence component.
//	Parameters:
//		- overrides References to override virtual methods
//		- tableName    (optional) a table name.
func InheritIdentifiableJsonPostgresPersistence[T any, K any](ctx context.Context, overrides IPostgresPersistenceOverrides[T], tableName string) *IdentifiableJsonPostgresPersistence[T, K] {
	c := &IdentifiableJsonPostgresPersistence[T, K]{}
	c.IdentifiablePostgresPersistence = *InheritIdentifiablePostgresPersistence[T, K](ctx, overrides, tableName)
	return c
}

// EnsureTable Adds DML statement to automatically create JSON(B) table
//	Parameters:
//   - idType type of the id column (default: TEXT)
//   - dataType type of the data column (default: JSONB)
func (c *IdentifiableJsonPostgresPersistence[T, K]) EnsureTable(idType string, dataType string) {
	if idType == "" {
		idType = "TEXT"
	}
	if dataType == "" {
		dataType = "JSONB"
	}

	query := "CREATE TABLE IF NOT EXISTS " + c.QuotedTableName() +
		" (\"id\" " + idType + " PRIMARY KEY, \"data\" " + dataType + ")"
	c.EnsureSchema(query)
}

// ConvertToPublic converts object value from internal to public format.
//	Parameters:
//		- value an object in internal format to convert.
//	Returns: converted object in public format.
func (c *IdentifiableJsonPostgresPersistence[T, K]) ConvertToPublic(rows pgx.Rows) T {

	var defaultValue T
	values, valErr := rows.Values()
	if valErr != nil || values == nil {
		return defaultValue
	}
	columns := rows.FieldDescriptions()

	buf := make(map[string]any, 0)

	for index, column := range columns {
		buf[(string)(column.Name)] = values[index]
	}

	item, ok := buf["data"]
	if !ok {
		item = buf
	}

	_buf, _ := cconv.JsonConverter.ToJson(item)
	_item, _ := c.IdentifiablePostgresPersistence.JsonConvertor.FromJson(_buf)
	return _item
}

// ConvertFromPublic convert object value from public to internal format.
//	Parameters:
//    - value     an object in public format to convert.
// Returns converted object in internal format.
func (c *IdentifiableJsonPostgresPersistence[T, K]) ConvertFromPublic(value T) map[string]any {
	id := cmpersist.GetObjectId(value)

	result := map[string]any{
		"id":   id,
		"data": value,
	}
	return result
}

// ConvertFromPublicPartial convert object value from public to internal format.
//	Parameters:
//		- value     an object in public format to convert.
//	Returns: converted object in internal format.
func (c *IdentifiableJsonPostgresPersistence[T, K]) ConvertFromPublicPartial(value map[string]any) map[string]any {
	buf, _ := cconv.JsonConverter.ToJson(value)
	item, _ := c.IdentifiablePostgresPersistence.JsonConvertor.FromJson(buf)
	return c.ConvertFromPublic(item)
}

// UpdatePartially updates only few selected fields in a data item.
//	Parameters:
//		- ctx context.Context
//		- correlation_id    (optional) transaction id to trace execution through call chain.
//		- id                an id of data item to be updated.
//		- data              a map with fields to be updated.
// Returns: receives updated item or error.
func (c *IdentifiableJsonPostgresPersistence[T, K]) UpdatePartially(ctx context.Context, correlationId string,
	id K, data cdata.AnyValueMap) (result T, err error) {

	query := "UPDATE " + c.QuotedTableName() + " SET \"data\"=\"data\"||$2 WHERE \"id\"=$1 RETURNING *"
	values := []any{id, data.Value()}

	rows, err := c.IdentifiablePostgresPersistence.Client.Query(ctx, query, values...)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	if !rows.Next() {
		return result, rows.Err()
	}

	_values, err := rows.Values()
	if err == nil && len(_values) > 0 {
		result = c.IdentifiablePostgresPersistence.Overrides.ConvertToPublic(rows)
		c.IdentifiablePostgresPersistence.Logger.Trace(ctx, correlationId, "Updated partially in %s with id = %s", c.IdentifiablePostgresPersistence.TableName, id)
		return result, nil
	}
	return result, rows.Err()
}
