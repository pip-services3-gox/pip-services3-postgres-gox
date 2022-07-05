package persistence

import (
	"context"
	"strconv"

	cconv "github.com/pip-services3-gox/pip-services3-commons-gox/convert"
	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	cmpersist "github.com/pip-services3-gox/pip-services3-data-gox/persistence"
)

// IdentifiablePostgresPersistence Abstract persistence component that stores data in PostgreSQL
// and implements a number of CRUD operations over data items with unique ids.
// The data items must implement IIdentifiable interface.
//
// In basic scenarios child classes shall only override getPageByFilter,
// getListByFilter or deleteByFilter operations with specific filter function.
// All other operations can be used out of the box.
//
// In complex scenarios child classes can implement additional operations by
// accessing c._collection and c._model properties.
//
//	### Configuration parameters ###
//		- collection:               (optional) PostgreSQL collection name
//		- connection(s):
//			- discovery_key:        (optional) a key to retrieve the connection from IDiscovery
//			- host:                 host name or IP address
//			- port:                 port number (default: 27017)
//			- uri:                  resource URI or connection string with all parameters in it
//		- credential(s):
//			- store_key:            (optional) a key to retrieve the credentials from ICredentialStore
//			- username:             (optional) user name
//			- password:             (optional) user password
//		- options:
//			- connect_timeout:      (optional) number of milliseconds to wait before timing out when connecting a new client (default: 0)
//			- idle_timeout:         (optional) number of milliseconds a client must sit idle in the pool and not be checked out (default: 10000)
//			- max_pool_size:        (optional) maximum number of clients the pool should contain (default: 10)
//
//### References ###
//
//- \*:logger:\*:\*:1.0           (optional) ILogger components to pass log messages components to pass log messages
//- \*:discovery:\*:\*:1.0        (optional) IDiscovery services
//- \*:credential-store:\*:\*:1.0 (optional) Credential stores to resolve credentials
// *
//### Example ###
type IdentifiablePostgresPersistence[T any, K any] struct {
	*PostgresPersistence[T]
}

// InheritIdentifiablePostgresPersistence creates a new instance of the persistence component.
//	Parameters:
//		- overrides References to override virtual methods
//		- tableName    (optional) a table name.
func InheritIdentifiablePostgresPersistence[T any, K any](overrides IPostgresPersistenceOverrides[T], tableName string) *IdentifiablePostgresPersistence[T, K] {
	if tableName == "" {
		panic("Table name could not be empty")
	}

	c := &IdentifiablePostgresPersistence[T, K]{}
	c.PostgresPersistence = InheritPostgresPersistence[T](overrides, tableName)

	return c
}

// GetListByIds gets a list of data items retrieved by given unique ids.
//	Parameters:
//		- ctx context.Context
//		- correlationId     (optional) transaction id to trace execution through call chain.
//		- ids of data items to be retrieved
//	Returns: a data list or error.
func (c *IdentifiablePostgresPersistence[T, K]) GetListByIds(ctx context.Context, correlationId string,
	ids []K) (items []T, err error) {

	params := GenerateParameters[K](ids)
	query := "SELECT * FROM " + c.QuotedTableName() + " WHERE \"id\" IN(" + params + ")"

	_ids := make([]any, len(ids), len(ids))
	for i := range ids {
		_ids[i] = ids[i]
	}

	qResult, qErr := c.Client.Query(ctx, query, _ids...)
	if qErr != nil {
		return nil, qErr
	}
	defer qResult.Close()
	items = make([]T, 0, 0)
	for qResult.Next() {

		item := c.Overrides.ConvertToPublic(qResult)
		items = append(items, item)
	}

	if items != nil {
		c.Logger.Trace(ctx, correlationId, "Retrieved %d from %s", len(items), c.TableName)
	}

	return items, qResult.Err()
}

// GetOneById gets a data item by its unique id.
//	Parameters:
//		- ctx context.Context
//		- correlationId     (optional) transaction id to trace execution through call chain.
//		- id                an id of data item to be retrieved.
// Returns: data item or error.
func (c *IdentifiablePostgresPersistence[T, K]) GetOneById(ctx context.Context, correlationId string, id K) (item T, err error) {

	query := "SELECT * FROM " + c.QuotedTableName() + " WHERE \"id\"=$1"

	qResult, qErr := c.Client.Query(ctx, query, id)
	if qErr != nil {
		return item, qErr
	}
	defer qResult.Close()
	if !qResult.Next() {
		return item, qResult.Err()
	}
	rows, vErr := qResult.Values()
	if vErr == nil && len(rows) > 0 {
		return c.Overrides.ConvertToPublic(qResult), nil
		//if item == nil {
		//	c.Logger.Trace(ctx, correlationId, "Nothing found from %s with id = %s", c.TableName, id)
		//} else {
		//	c.Logger.Trace(ctx, correlationId, "Retrieved from %s with id = %s", c.TableName, id)
		//}
	}
	return item, vErr
}

// Create a data item.
//	Parameters:
//		- ctx context.Context
//		- correlation_id    (optional) transaction id to trace execution through call chain.
//		- item              an item to be created.
//	Returns: (optional)  created item or error.
func (c *IdentifiablePostgresPersistence[T, K]) Create(ctx context.Context, correlationId string, item T) (result T, err error) {
	// TODO::fixme
	// Assign unique id
	var newItem any = item
	//newItem = cmpersist.CloneObject(item, c.Prototype)
	cmpersist.GenerateObjectId(&newItem)
	item = newItem.(T)

	return c.PostgresPersistence.Create(ctx, correlationId, item)
}

// Set a data item. If the data item exists it updates it,
// otherwise it creates a new data item.
//	Parameters:
//		- ctx context.Context
//		- correlation_id    (optional) transaction id to trace execution through call chain.
//		- item              an item to be set.
//	Returns: (optional)  updated item or error.
func (c *IdentifiablePostgresPersistence[T, K]) Set(ctx context.Context, correlationId string, item T) (result T, err error) {
	// TODO::fixme

	// Assign unique id
	var newItem any = item
	//newItem = cmpersist.CloneObject(item, c.Prototype)
	cmpersist.GenerateObjectId(&newItem)

	row := c.Overrides.ConvertFromPublic(item)
	params := c.GenerateParameters(row)
	setParams, columns := c.GenerateSetParameters(row)
	values := c.GenerateValues(columns, row)
	id := cmpersist.GetObjectId(newItem)

	query := "INSERT INTO " + c.QuotedTableName() + " (" + columns + ")" +
		" VALUES (" + params + ")" +
		" ON CONFLICT (\"id\") DO UPDATE SET " + setParams + " RETURNING *"

	qResult, qErr := c.Client.Query(ctx, query, values...)
	if qErr != nil {
		return result, qErr
	}
	defer qResult.Close()

	if !qResult.Next() {
		return result, qResult.Err()
	}
	rows, vErr := qResult.Values()
	if vErr == nil && len(rows) > 0 {
		result = c.Overrides.ConvertToPublic(qResult)
		c.Logger.Trace(ctx, correlationId, "Set in %s with id = %s", c.TableName, id)
		return result, nil
	}
	return result, vErr

}

// Update a data item.
//	Parameters:
//		- ctx context.Context
//		- correlation_id    (optional) transaction id to trace execution through call chain.
//		- item              an item to be updated.
//	Returns          (optional)  updated item or error.
func (c *IdentifiablePostgresPersistence[T, K]) Update(ctx context.Context, correlationId string, item T) (result T, err error) {

	var newItem any = item
	//newItem = cmpersist.CloneObject(item, c.Prototype)
	id := cmpersist.GetObjectId(newItem)
	item = newItem.(T)

	row := c.Overrides.ConvertFromPublic(item)
	params, col := c.GenerateSetParameters(row)
	values := c.GenerateValues(col, row)
	values = append(values, id)

	query := "UPDATE " + c.QuotedTableName() +
		" SET " + params + " WHERE \"id\"=$" + strconv.FormatInt((int64)(len(values)), 10) + " RETURNING *"

	qResult, qErr := c.Client.Query(ctx, query, values...)

	if qErr != nil {
		return result, qErr
	}
	defer qResult.Close()
	if !qResult.Next() {
		return result, qResult.Err()
	}
	rows, vErr := qResult.Values()
	if vErr == nil && len(rows) > 0 {
		result = c.Overrides.ConvertToPublic(qResult)
		c.Logger.Trace(ctx, correlationId, "Updated in %s with id = %s", c.TableName, id)
		return result, nil
	}
	return result, vErr
}

// UpdatePartially updates only few selected fields in a data item.
//	Parameters:
//		- ctx context.Context
//		- correlation_id    (optional) transaction id to trace execution through call chain.
//		- id                an id of data item to be updated.
//		- data              a map with fields to be updated.
//	Returns: updated item or error.
func (c *IdentifiablePostgresPersistence[T, K]) UpdatePartially(ctx context.Context, correlationId string, id K, data cdata.AnyValueMap) (result T, err error) {

	row := c.Overrides.ConvertFromPublicPartial(data.Value())
	params, col := c.GenerateSetParameters(row)
	values := c.GenerateValues(col, row)
	values = append(values, id)

	query := "UPDATE " + c.QuotedTableName() +
		" SET " + params + " WHERE \"id\"=$" + strconv.FormatInt((int64)(len(values)), 10) + " RETURNING *"

	qResult, qErr := c.Client.Query(ctx, query, values...)

	if qErr != nil {
		return result, qErr
	}
	defer qResult.Close()
	if !qResult.Next() {
		return result, qResult.Err()
	}
	rows, vErr := qResult.Values()
	if vErr == nil && len(rows) > 0 {
		result = c.Overrides.ConvertToPublic(qResult)
		c.Logger.Trace(ctx, correlationId, "Updated partially in %s with id = %s", c.TableName, id)
		return result, nil
	}
	return result, vErr
}

// DeleteById deletes a data item by its unique id.
//	Parameters:
//		- ctx context.Context
//		- correlation_id    (optional) transaction id to trace execution through call chain.
//		- id                an id of the item to be deleted
//	Returns: (optional)  deleted item or error.
func (c *IdentifiablePostgresPersistence[T, K]) DeleteById(ctx context.Context, correlationId string, id K) (result T, err error) {

	query := "DELETE FROM " + c.QuotedTableName() + " WHERE \"id\"=$1 RETURNING *"

	qResult, qErr := c.Client.Query(ctx, query, id)

	if qErr != nil {
		return result, qErr
	}
	defer qResult.Close()
	if !qResult.Next() {
		return result, qResult.Err()
	}
	rows, vErr := qResult.Values()
	if vErr == nil && len(rows) > 0 {
		result = c.Overrides.ConvertToPublic(qResult)
		c.Logger.Trace(ctx, correlationId, "Deleted from %s with id = %s", c.TableName, id)
		return result, nil
	}
	return result, vErr
}

// DeleteByIds deletes multiple data items by their unique ids.
//	Parameters:
//		- ctx context.Context
//		- correlationId     (optional) transaction id to trace execution through call chain.
//		- ids                of data items to be deleted.
//	Returns: (optional)  error or null for success.
func (c *IdentifiablePostgresPersistence[T, K]) DeleteByIds(ctx context.Context, correlationId string, ids []K) error {

	params := GenerateParameters[K](ids)
	query := "DELETE FROM " + c.QuotedTableName() + " WHERE \"id\" IN(" + params + ")"

	_ids := make([]any, len(ids), len(ids))
	for i := range ids {
		_ids[i] = ids[i]
	}
	qResult, qErr := c.Client.Query(ctx, query, _ids...)

	if qErr != nil {
		return qErr
	}
	defer qResult.Close()
	if !qResult.Next() {
		return qResult.Err()
	}
	var count int64 = 0
	rows, vErr := qResult.Values()
	if vErr == nil && len(rows) == 1 {
		count = cconv.LongConverter.ToLong(rows[0])
		if count != 0 {
			c.Logger.Trace(ctx, correlationId, "Deleted %d items from %s", count, c.TableName)
		}
	}
	return vErr
}
