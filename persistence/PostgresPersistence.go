package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v4"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	cconf "github.com/pip-services3-gox/pip-services3-commons-gox/config"
	cconv "github.com/pip-services3-gox/pip-services3-commons-gox/convert"
	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	cerr "github.com/pip-services3-gox/pip-services3-commons-gox/errors"
	cref "github.com/pip-services3-gox/pip-services3-commons-gox/refer"
	clog "github.com/pip-services3-gox/pip-services3-components-gox/log"
	cmpersist "github.com/pip-services3-gox/pip-services3-data-gox/persistence"
	conn "github.com/pip-services3-gox/pip-services3-postgres-gox/connect"
)

type IPostgresPersistenceOverrides[T any] interface {
	DefineSchema()
	ConvertFromPublic(item T) map[string]any
	ConvertToPublic(item pgx.Rows) T
	ConvertFromPublicPartial(item map[string]any) map[string]any
}

// PostgresPersistence Abstract persistence component that stores data in PostgreSQL using plain driver.
//
// This is the most basic persistence component that is only
// able to store data items of any type. Specific CRUD operations
// over the data items must be implemented in child classes by
// accessing c._db or c._collection properties.
//
//	### Configuration parameters ###
//		- collection:                  (optional) PostgreSQL collection name
//		- schema:                  	   (optional) PostgreSQL schema, default "public"
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
//		- \*:logger:\*:\*:1.0           (optional) ILogger components to pass log messages
//		- \*:discovery:\*:\*:1.0        (optional) IDiscovery services
//		- \*:credential-store:\*:\*:1.0 (optional) Credential stores to resolve credentials
//
//	### Example ###
type PostgresPersistence[T any] struct {
	Overrides        IPostgresPersistenceOverrides[T]
	JsonConvertor    cconv.IJSONEngine[T]
	JsonMapConvertor cconv.IJSONEngine[map[string]any]

	defaultConfig *cconf.ConfigParams

	config           *cconf.ConfigParams
	references       cref.IReferences
	opened           bool
	localConnection  bool
	schemaStatements []string

	//The dependency resolver.
	DependencyResolver *cref.DependencyResolver
	//The logger.
	Logger *clog.CompositeLogger
	//The PostgreSQL connection component.
	Connection *conn.PostgresConnection
	//The PostgreSQL connection pool object.
	Client *pgxpool.Pool
	//The PostgreSQL database name.
	DatabaseName string
	//The PostgreSQL database schema name. If not set use "public" by default
	SchemaName string
	//The PostgreSQL table object.
	TableName   string
	MaxPageSize int
}

// InheritPostgresPersistence creates a new instance of the persistence component.
//	Parameters:
//		- overrides References to override virtual methods
//		- tableName    (optional) a table name.
func InheritPostgresPersistence[T any](overrides IPostgresPersistenceOverrides[T], tableName string) *PostgresPersistence[T] {
	c := &PostgresPersistence[T]{
		Overrides: overrides,
		defaultConfig: cconf.NewConfigParamsFromTuples(
			"collection", nil,
			"dependencies.connection", "*:connection:postgres:*:1.0",
			"options.max_pool_size", 2,
			"options.keep_alive", 1,
			"options.connect_timeout", 5000,
			"options.auto_reconnect", true,
			"options.max_page_size", 100,
			"options.debug", true,
		),
		schemaStatements: make([]string, 0),
		Logger:           clog.NewCompositeLogger(),
		MaxPageSize:      100,
		TableName:        tableName,
		JsonConvertor:    cconv.NewDefaultCustomTypeJsonConvertor[T](),
		JsonMapConvertor: cconv.NewDefaultCustomTypeJsonConvertor[map[string]any](),
	}

	c.DependencyResolver = cref.NewDependencyResolver()
	c.DependencyResolver.Configure(context.TODO(), c.defaultConfig)

	return c
}

// Configure component by passing configuration parameters.
//	Parameters:
//		- ctx context.Context
//		- config configuration parameters to be set.
func (c *PostgresPersistence[T]) Configure(ctx context.Context, config *cconf.ConfigParams) {
	config = config.SetDefaults(c.defaultConfig)
	c.config = config

	c.DependencyResolver.Configure(ctx, config)

	c.TableName = config.GetAsStringWithDefault("collection", c.TableName)
	c.TableName = config.GetAsStringWithDefault("table", c.TableName)
	c.MaxPageSize = config.GetAsIntegerWithDefault("options.max_page_size", c.MaxPageSize)
	c.SchemaName = config.GetAsStringWithDefault("schema", c.SchemaName)
}

// SetReferences to dependent components.
//	Parameters:
//		- ctx context.Context
//		- references references to locate the component dependencies.
func (c *PostgresPersistence[T]) SetReferences(ctx context.Context, references cref.IReferences) {
	c.references = references
	c.Logger.SetReferences(ctx, references)

	// Get connection
	c.DependencyResolver.SetReferences(ctx, references)
	result := c.DependencyResolver.GetOneOptional("connection")
	if dep, ok := result.(*conn.PostgresConnection); ok {
		c.Connection = dep
	}
	// Or create a local one
	if c.Connection == nil {
		c.Connection = c.createConnection(ctx)
		c.localConnection = true
	} else {
		c.localConnection = false
	}
}

// UnsetReferences (clears) previously set references to dependent components.
func (c *PostgresPersistence[T]) UnsetReferences() {
	c.Connection = nil
}

func (c *PostgresPersistence[T]) createConnection(ctx context.Context) *conn.PostgresConnection {
	connection := conn.NewPostgresConnection()
	if c.config != nil {
		connection.Configure(ctx, c.config)
	}
	if c.references != nil {
		connection.SetReferences(ctx, c.references)
	}
	return connection
}

// EnsureIndex adds index definition to create it on opening
//	Parameters:
//		- keys index keys (fields)
//		- options index options
func (c *PostgresPersistence[T]) EnsureIndex(name string, keys map[string]string, options map[string]string) {
	builder := "CREATE"
	if options == nil {
		options = make(map[string]string, 0)
	}

	if options["unique"] != "" {
		builder += " UNIQUE"
	}

	indexName := c.QuoteIdentifier(name)
	if len(c.SchemaName) > 0 {
		indexName = c.QuoteIdentifier(c.SchemaName) + "." + indexName
	}

	builder += " INDEX IF NOT EXISTS " + indexName + " ON " + c.QuotedTableName()

	if options["type"] != "" {
		builder += " " + options["type"]
	}

	fields := ""
	for key, _ := range keys {
		if fields != "" {
			fields += ", "
		}
		fields += key
		asc := keys[key]
		if asc != "1" {
			fields += " DESC"
		}
	}

	builder += "(" + fields + ")"

	c.EnsureSchema(builder)
}

// DefineSchema a database schema for this persistence, have to call in child class
func (c *PostgresPersistence[T]) DefineSchema() {
	// Override in child classes

	if len(c.SchemaName) > 0 {
		c.EnsureSchema("CREATE SCHEMA IF NOT EXISTS " + c.QuoteIdentifier(c.SchemaName))
	}
}

// EnsureSchema adds a statement to schema definition
//	Parameters:
//   - schemaStatement a statement to be added to the schema
func (c *PostgresPersistence[T]) EnsureSchema(schemaStatement string) {
	c.schemaStatements = append(c.schemaStatements, schemaStatement)
}

// ClearSchema clears all auto-created objects
func (c *PostgresPersistence[T]) ClearSchema() {
	c.schemaStatements = []string{}
}

// ConvertToPublic converts object value from internal to func (c * PostgresPersistence) format.
//	Parameters:
//		- value an object in internal format to convert.
//	Returns: converted object in func (c * PostgresPersistence) format.
func (c *PostgresPersistence[T]) ConvertToPublic(rows pgx.Rows) T {
	// TODO::fixme
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
	jsonBuf, _ := cconv.JsonConverter.ToJson(buf)
	item, _ := c.JsonConvertor.FromJson(jsonBuf)
	return item

}

// ConvertFromPublic сonvert object value from func (c * PostgresPersistence) to internal format.
//	Parameters:
//		- value an object in func (c * PostgresPersistence) format to convert.
//	Returns: converted object in internal format.
func (c *PostgresPersistence[T]) ConvertFromPublic(value T) map[string]any {
	buf, _ := cconv.JsonConverter.ToJson(value)
	item, _ := c.JsonMapConvertor.FromJson(buf)
	return item
}

// ConvertFromPublicPartial converts the given object from the public partial format.
//	Parameters:
//		- value the object to convert from the public partial format.
//	Returns: the initial object.
func (c *PostgresPersistence[T]) ConvertFromPublicPartial(value map[string]any) map[string]any {
	buf, _ := cconv.JsonConverter.ToJson(value)
	item, _ := c.JsonMapConvertor.FromJson(buf)
	return item
}

func (c *PostgresPersistence[T]) QuoteIdentifier(value string) string {
	if value == "" {
		return value
	}
	if value[0] == '\'' {
		return value
	}
	return "\"" + value + "\""
}

// QuotedTableName return quoted SchemaName with TableName ("schema"."table")
func (c *PostgresPersistence[T]) QuotedTableName() string {
	if len(c.SchemaName) > 0 {
		return c.QuoteIdentifier(c.SchemaName) + "." + c.QuoteIdentifier(c.TableName)
	}
	return c.QuoteIdentifier(c.TableName)
}

// IsOpen checks if the component is opened.
//	Returns: true if the component has been opened and false otherwise.
func (c *PostgresPersistence[T]) IsOpen() bool {
	return c.opened
}

// Open the component.
//	Parameters:
//		- ctx context.Context
//		- correlationId (optional) transaction id to trace execution through call chain.
//	Returns: error or nil no errors occurred.
func (c *PostgresPersistence[T]) Open(ctx context.Context, correlationId string) (err error) {
	if c.opened {
		return nil
	}

	if c.Connection == nil {
		c.Connection = c.createConnection(ctx)
		c.localConnection = true
	}

	if c.localConnection {
		err = c.Connection.Open(ctx, correlationId)
	}

	if err == nil && c.Connection == nil {
		err = cerr.NewInvalidStateError(correlationId, "NO_CONNECTION", "PostgreSQL connection is missing")
	}

	if err == nil && !c.Connection.IsOpen() {
		err = cerr.NewConnectionError(correlationId, "CONNECT_FAILED", "PostgreSQL connection is not opened")
	}

	c.opened = false

	if err != nil {
		return err
	}
	c.Client = c.Connection.GetConnection()
	c.DatabaseName = c.Connection.GetDatabaseName()

	// Define database schema
	c.Overrides.DefineSchema()

	// Recreate objects
	err = c.CreateSchema(ctx, correlationId)
	if err != nil {
		c.Client = nil
		err = cerr.NewConnectionError(correlationId, "CONNECT_FAILED", "Connection to postgres failed").WithCause(err)
	} else {
		c.opened = true
		c.Logger.Debug(ctx, correlationId, "Connected to postgres database %s, collection %s", c.DatabaseName, c.QuotedTableName())
	}

	return err

}

// Close component and frees used resources.
//	Parameters:
//		- ctx context.Context
//		- correlationId (optional) transaction id to trace execution through call chain.
//	Returns: error or nil no errors occurred.
func (c *PostgresPersistence[T]) Close(ctx context.Context, correlationId string) (err error) {
	if !c.opened {
		return nil
	}

	if c.Connection == nil {
		return cerr.NewInvalidStateError(correlationId, "NO_CONNECTION", "Postgres connection is missing")
	}

	if c.localConnection {
		err = c.Connection.Close(ctx, correlationId)
	}
	if err != nil {
		return err
	}
	c.opened = false
	c.Client = nil
	return nil
}

// Clear component state.
//	Parameters:
//		- ctx context.Context
//		- correlationId 	(optional) transaction id to trace execution through call chain.
//	Returns: error or nil no errors occured.
func (c *PostgresPersistence[T]) Clear(ctx context.Context, correlationId string) error {
	// Return error if collection is not set
	if c.TableName == "" {
		return errors.New("Table name is not defined")
	}

	query := "DELETE FROM " + c.QuotedTableName()

	qResult, err := c.Client.Query(ctx, query)
	if err != nil {
		err = cerr.NewConnectionError(correlationId, "CONNECT_FAILED", "Connection to postgres failed").
			WithCause(err)
	}
	defer qResult.Close()
	return err
}

func (c *PostgresPersistence[T]) CreateSchema(ctx context.Context, correlationId string) (err error) {
	// TODO::fixme
	if c.schemaStatements == nil || len(c.schemaStatements) == 0 {
		return nil
	}

	// Check if table exist to determine weither to auto create objects
	query := "SELECT to_regclass('" + c.QuotedTableName() + "')"
	qResult, qErr := c.Client.Query(ctx, query)
	if qErr != nil {
		return qErr
	}
	defer qResult.Close()
	// If table already exists then exit
	if qResult != nil && qResult.Next() {
		val, cErr := qResult.Values()
		if cErr != nil {
			return cErr
		}

		if len(val) > 0 && val[0] == c.TableName {
			return nil
		}
	}
	c.Logger.Debug(ctx, correlationId, "Table "+c.QuotedTableName()+" does not exist. Creating database objects...")
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, dml := range c.schemaStatements {
			qResult, err := c.Client.Query(ctx, dml)
			if err != nil {
				c.Logger.Error(ctx, correlationId, err, "Failed to autocreate database object")
			}
			if qResult != nil {
				qResult.Close()
			}
		}
	}()
	wg.Wait()
	return qResult.Err()
}

// GenerateColumns generates a list of column names to use in SQL statements like: "column1,column2,column3"
//	Parameters:
//		- values an array with column values or a key-value map
//	Returns: a generated list of column names
func (c *PostgresPersistence[T]) GenerateColumns(values any) string {

	items := c.convertToMap(values)
	if items == nil {
		return ""
	}
	result := strings.Builder{}
	for item := range items {
		if result.String() != "" {
			result.WriteString(",")
		}
		result.WriteString(c.QuoteIdentifier(item))
	}
	return result.String()

}

// GenerateParameters generates a list of value parameters to use in SQL statements like: "$1,$2,$3"
//	Parameters:
//		- values an array with column values or a key-value map
//	Returns: a generated list of value parameters
func (c *PostgresPersistence[T]) GenerateParameters(values any) string {

	result := strings.Builder{}
	// String arrays
	if val, ok := values.([]any); ok {
		for index := 1; index <= len(val); index++ {
			if result.String() != "" {
				result.WriteString(",")
			}
			result.WriteString("$")
			result.WriteString(strconv.FormatInt((int64)(index), 10))
		}

		return result.String()
	}

	items := c.convertToMap(values)
	if items == nil {
		return ""
	}

	for index := 1; index <= len(items); index++ {
		if result.String() != "" {
			result.WriteString(",")
		}
		result.WriteString("$")
		result.WriteString(strconv.FormatInt((int64)(index), 10))
	}

	return result.String()
}

// GenerateSetParameters generates a list of column sets to use in UPDATE statements like: column1=$1,column2=$2
//	Parameters:
//		- values an array with column values or a key-value map
//	Returns: a generated list of column sets
func (c *PostgresPersistence[T]) GenerateSetParameters(values any) (setParams string, columns string) {

	items := c.convertToMap(values)
	if items == nil {
		return "", ""
	}
	setParamsBuf := strings.Builder{}
	colBuf := strings.Builder{}
	index := 1
	for column := range items {
		if setParamsBuf.String() != "" {
			setParamsBuf.WriteString(",")
			colBuf.WriteString(",")
		}
		setParamsBuf.WriteString(c.QuoteIdentifier(column) + "=$" + strconv.FormatInt((int64)(index), 10))
		colBuf.WriteString(c.QuoteIdentifier(column))
		index++
	}
	return setParamsBuf.String(), colBuf.String()
}

// GenerateValues generates a list of column parameters
//	Parameters:
//		- values an array with column values or a key-value map
//	Returns: a generated list of column values
func (c *PostgresPersistence[T]) GenerateValues(columns string, values any) []any {
	results := make([]any, 0, 1)

	items := c.convertToMap(values)
	if items == nil {
		return nil
	}

	if columns == "" {
		panic("GenerateValues: Columns must be set for properly convert")
	}

	columnNames := strings.Split(strings.ReplaceAll(columns, "\"", ""), ",")
	for _, item := range columnNames {
		results = append(results, items[item])
	}
	return results
}

func (c *PostgresPersistence[T]) convertToMap(values any) map[string]any {
	mRes, mErr := json.Marshal(values)
	if mErr != nil {
		c.Logger.Error(context.TODO(), "PostgresPersistence", mErr, "Error data convertion")
		return nil
	}
	items := make(map[string]any, 0)
	mErr = json.Unmarshal(mRes, &items)
	if mErr != nil {
		c.Logger.Error(context.TODO(), "PostgresPersistence", mErr, "Error data convertion")
		return nil
	}
	return items
}

// GetPageByFilter gets a page of data items retrieved by a given filter and sorted according to sort parameters.
// This method shall be called by a func (c * PostgresPersistence) getPageByFilter method from child class that
// receives FilterParams and converts them into a filter function.
//	Parameters:
//		- ctx context.Context
//		- correlationId     (optional) transaction id to trace execution through call chain.
//		- filter            (optional) a filter JSON object
//		- paging            (optional) paging parameters
//		- sort              (optional) sorting JSON object
//		- select            (optional) projection JSON object
//	Returns: receives a data page or error.
func (c *PostgresPersistence[T]) GetPageByFilter(ctx context.Context, correlationId string,
	filter any, paging cdata.PagingParams, sort any, sel any) (page cdata.DataPage[T], err error) {

	query := "SELECT * FROM " + c.QuotedTableName()
	if sel != nil {
		if slct, ok := sel.(string); ok && slct != "" {
			query = "SELECT " + slct + " FROM " + c.QuotedTableName()
		}
	}

	// Adjust max item count based on configuration paging
	skip := paging.GetSkip(-1)
	take := paging.GetTake((int64)(c.MaxPageSize))
	pagingEnabled := paging.Total

	if filter != nil {
		if flt, ok := filter.(string); ok && flt != "" {
			query += " WHERE " + flt
		}
	}

	if sort != nil {
		if srt, ok := sort.(string); ok && srt != "" {
			query += " ORDER BY " + srt
		}
	}

	if skip >= 0 {
		query += " OFFSET " + strconv.FormatInt(skip, 10)
	}

	query += " LIMIT " + strconv.FormatInt(take, 10)
	qResult, qErr := c.Client.Query(context.TODO(), query)

	if qErr != nil {
		return *cdata.NewEmptyDataPage[T](), qErr
	}

	defer qResult.Close()

	items := make([]T, 0, 0)
	for qResult.Next() {
		item := c.Overrides.ConvertToPublic(qResult)
		items = append(items, item)
	}

	if items != nil {
		c.Logger.Trace(ctx, correlationId, "Retrieved %d from %s", len(items), c.TableName)
	}

	if pagingEnabled {
		query := "SELECT COUNT(*) AS count FROM " + c.QuotedTableName()
		if filter != nil {
			if flt, ok := filter.(string); ok && flt != "" {
				query += " WHERE " + flt
			}
		}

		qResult2, qErr2 := c.Client.Query(ctx, query)
		if qErr2 != nil {
			return *cdata.NewEmptyDataPage[T](), qErr2
		}
		defer qResult2.Close()
		var count int64 = 0
		if qResult2.Next() {
			rows, _ := qResult2.Values()
			if len(rows) == 1 {
				count = cconv.LongConverter.ToLong(rows[0])
			}
		}
		page = *cdata.NewDataPage[T](items, int(count))
		return page, qResult2.Err()
	}

	page = *cdata.NewDataPage[T](items, cdata.EmptyTotalValue)
	return page, qResult.Err()
}

// GetCountByFilter gets a number of data items retrieved by a given filter.
// This method shall be called by a func (c * PostgresPersistence) getCountByFilter method from child class that
// receives FilterParams and converts them into a filter function.
//	Parameters:
//		- ctx context.Context
//		- correlationId     (optional) transaction id to trace execution through call chain.
//		- filter            (optional) a filter JSON object
//	Returns: data page or error.
func (c *PostgresPersistence[T]) GetCountByFilter(ctx context.Context, correlationId string,
	filter any) (count int64, err error) {

	query := "SELECT COUNT(*) AS count FROM " + c.QuotedTableName()

	if filter != nil {
		if flt, ok := filter.(string); ok && flt != "" {
			query += " WHERE " + flt
		}
	}

	qResult, qErr := c.Client.Query(ctx, query)
	if qErr != nil {
		return 0, qErr
	}
	defer qResult.Close()
	count = 0
	if qResult != nil && qResult.Next() {
		rows, _ := qResult.Values()
		if len(rows) == 1 {
			count = cconv.LongConverter.ToLong(rows[0])
		}
	}
	if count != 0 {
		c.Logger.Trace(ctx, correlationId, "Counted %d items in %s", count, c.TableName)
	}

	return count, qResult.Err()
}

// GetListByFilter gets a list of data items retrieved by a given filter and sorted according to sort parameters.
// This method shall be called by a func (c * PostgresPersistence) getListByFilter method from child class that
// receives FilterParams and converts them into a filter function.
//	Parameters:
//		- ctx context.Context
//		- correlationId    (optional) transaction id to trace execution through call chain.
//		- filter           (optional) a filter JSON object
//		- paging           (optional) paging parameters
//		- sort             (optional) sorting JSON object
//		- select           (optional) projection JSON object
//	Returns: data list or error.
func (c *PostgresPersistence[T]) GetListByFilter(ctx context.Context, correlationId string,
	filter any, sort any, sel any) (items []T, err error) {

	query := "SELECT * FROM " + c.QuotedTableName()
	if sel != nil {
		if slct, ok := sel.(string); ok && slct != "" {
			query = "SELECT " + slct + " FROM " + c.QuotedTableName()
		}
	}

	if filter != nil {
		if flt, ok := filter.(string); ok && flt != "" {
			query += " WHERE " + flt
		}
	}

	if sort != nil {
		if srt, ok := sort.(string); ok && srt != "" {
			query += " ORDER BY " + srt
		}
	}

	qResult, qErr := c.Client.Query(ctx, query)

	if qErr != nil {
		return nil, qErr
	}
	defer qResult.Close()
	items = make([]T, 0, 1)
	for qResult.Next() {
		item := c.Overrides.ConvertToPublic(qResult)
		items = append(items, item)
	}

	if items != nil {
		c.Logger.Trace(ctx, correlationId, "Retrieved %d from %s", len(items), c.TableName)
	}
	return items, qResult.Err()
}

// GetOneRandom gets a random item from items that match to a given filter.
// This method shall be called by a func (c * PostgresPersistence) getOneRandom method from child class that
// receives FilterParams and converts them into a filter function.
//	Parameters:
//		- ctx context.Context
//		- correlationId     (optional) transaction id to trace execution through call chain.
//		- filter            (optional) a filter JSON object
//	Returns: random item or error.
func (c *PostgresPersistence[T]) GetOneRandom(ctx context.Context, correlationId string, filter any) (item T, err error) {

	query := "SELECT COUNT(*) AS count FROM " + c.QuotedTableName()

	if filter != nil {
		if flt, ok := filter.(string); ok && flt != "" {
			query += " WHERE " + flt
		}
	}

	qResult, qErr := c.Client.Query(ctx, query)
	if qErr != nil {
		return item, qErr
	}
	defer qResult.Close()

	query = "SELECT * FROM " + c.QuotedTableName()
	if filter != nil {
		if flt, ok := filter.(string); ok && flt != "" {
			query += " WHERE " + flt
		}
	}

	var count int64 = 0
	if !qResult.Next() {
		return item, qResult.Err()
	}
	rows, _ := qResult.Values()
	if len(rows) == 1 {
		if row, ok := rows[0].(int64); ok {
			count = row
		} else {
			count = 0
		}
	}

	if count == 0 {
		c.Logger.Trace(ctx, correlationId, "Can't retriev random item from %s. Table is empty.", c.TableName)
		return item, nil
	}

	rand.Seed(time.Now().UnixNano())
	pos := rand.Int63n(int64(count))
	query += " OFFSET " + strconv.FormatInt(pos, 10) + " LIMIT 1"
	qResult2, qErr2 := c.Client.Query(ctx, query)
	if qErr2 != nil {
		return item, qErr
	}
	defer qResult2.Close()
	if !qResult2.Next() {
		c.Logger.Trace(ctx, correlationId, "Random item wasn't found from %s", c.TableName)
		return item, qResult2.Err()
	}
	item = c.Overrides.ConvertToPublic(qResult2)
	c.Logger.Trace(ctx, correlationId, "Retrieved random item from %s", c.TableName)
	return item, nil

}

// Create creates a data item.
//	Parameters:
//		- ctx context.Context
//		- correlation_id    (optional) transaction id to trace execution through call chain.
//		- item              an item to be created.
//	Returns: (optional) callback function that receives created item or error.
func (c *PostgresPersistence[T]) Create(ctx context.Context, correlationId string, item T) (result T, err error) {

	row := c.Overrides.ConvertFromPublic(item)
	columns := c.GenerateColumns(row)
	params := c.GenerateParameters(row)
	values := c.GenerateValues(columns, row)
	query := "INSERT INTO " + c.QuotedTableName() + " (" + columns + ") VALUES (" + params + ") RETURNING *"
	qResult, qErr := c.Client.Query(ctx, query, values...)
	if qErr != nil {
		return result, qErr
	}
	defer qResult.Close()
	if !qResult.Next() {
		return result, qResult.Err()
	}
	item = c.Overrides.ConvertToPublic(qResult)
	id := cmpersist.GetObjectId(item)
	c.Logger.Trace(ctx, correlationId, "Created in %s with id = %s", c.TableName, id)
	return item, nil

}

// DeleteByFilter deletes data items that match to a given filter.
// This method shall be called by a func (c * PostgresPersistence) deleteByFilter method from child class that
// receives FilterParams and converts them into a filter function.
//	Parameters:
//		- ctx context.Context
//		- correlationId     (optional) transaction id to trace execution through call chain.
//		- filter            (optional) a filter JSON object.
//	Returns: error or nil for success.
func (c *PostgresPersistence[T]) DeleteByFilter(ctx context.Context, correlationId string, filter string) (err error) {
	query := "DELETE FROM " + c.QuotedTableName()
	if filter != "" {
		query += " WHERE " + filter
	}

	qResult, qErr := c.Client.Query(ctx, query)
	defer qResult.Close()

	if qErr != nil {
		return qErr
	}
	defer qResult.Close()

	var count int64 = 0
	if !qResult.Next() {
		return qResult.Err()
	}
	rows, _ := qResult.Values()
	if len(rows) == 1 {
		count = cconv.LongConverter.ToLong(rows[0])
	}
	c.Logger.Trace(ctx, correlationId, "Deleted %d items from %s", count, c.TableName)
	return nil
}
