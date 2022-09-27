# <img src="https://uploads-ssl.webflow.com/5ea5d3315186cf5ec60c3ee4/5edf1c94ce4c859f2b188094_logo.svg" alt="Pip.Services Logo" width="200"> <br/> PostgreSQL components for Golang

This module is a part of the [Pip.Services](http://pipservices.org) polyglot microservices toolkit. It provides a set of components to implement PostgreSQL persistence.

The module contains the following packages:
- [**Build**](https://godoc.org/github.com/pip-services3-gox/pip-services3-postgres-gox/build) - Factory to create PostreSQL persistence components.
- [**Connect**](https://godoc.org/github.com/pip-services3-gox/pip-services3-postgres-gox/connect) - Connection component to configure PostgreSQL connection to database.
- [**Persistence**](https://godoc.org/github.com/pip-services3-gox/pip-services3-postgres-gox/persistence) - abstract persistence components to perform basic CRUD operations.

<a name="links"></a> Quick links:

* [Configuration](http://docs.pipservices.org/conceptual/configuration/component_configuration/)
* [API Reference](https://godoc.org/github.com/pip-services3-gox/pip-services3-postgres-gox/)
* [Change Log](CHANGELOG.md)
* [Get Help](http://docs.pipservices.org/get_help/)
* [Contribute](http://docs.pipservices.org/contribute/)

## Use

Get the package from the Github repository:
```bash
go get -u github.com/pip-services3-gox/pip-services3-postgres-gox@latest
```

As an example, lets create persistence for the following data object.

```go
type MyData struct {
	id      string `bson:"_id" json:"id"`
	key     string `bson:"key" json:"key"`
	content string `bson:"content" json:"content"`
}

func (c *MyData) GetId() string {
	return c.id
}
```

The persistence component shall implement the following interface with a basic set of CRUD operations.

```go
type IMyPersistence interface {
	GetPageByFilter(ctx context.Context, correlationId string, filter data.FilterParams, paging data.PagingParams) (page data.DataPage[MyData], err error)
	GetOneById(ctx context.Context, correlationId string, id string) (item MyData, err error)
	GetOneByKey(ctx context.Context, correlationId string, key string) (item MyData, err error)
	Create(ctx context.Context, correlationId string, item MyData) (result MyData, err error)
	Update(ctx context.Context, correlationId string, item MyData) (result MyData, err error)
	DeleteById(ctx context.Context, correlationId string, id string) (item MyData, err error)
}
```

To implement postgresql persistence component you shall inherit `IdentifiablePostgresPersistence`. 
Most CRUD operations will come from the base class. You only need to override `GetPageByFilter` method with a custom filter function.
And implement a `GetOneByKey` custom persistence method that doesn't exist in the base class.

```go

type MyPostgresPersistence struct {
	*persistence.IdentifiablePostgresPersistence[MyData, string]
}

func NewMyPostgresPersistence() *MyPostgresPersistence {
	c := &MyPostgresPersistence{}
	c.IdentifiablePostgresPersistence = persistence.InheritIdentifiablePostgresPersistence[MyData, string](c, "my_data")
	return c
}

func (c *MyPostgresPersistence) DefineSchema() {
	c.ClearSchema()
	c.IdentifiablePostgresPersistence.DefineSchema()
	// Row name must be in double quotes for properly case!!!
	c.EnsureSchema("CREATE TABLE " + c.QuotedTableName() + " (\"id\" TEXT PRIMARY KEY, \"key\" TEXT, \"content\" TEXT)")
	c.EnsureIndex(c.IdentifiablePostgresPersistence.TableName+"_key", map[string]string{"key": "1"}, map[string]string{"unique": "true"})
}

func (c *MyPostgresPersistence) composeFilter(filter data.FilterParams) string {
	if &filter == nil {
		filter = *data.NewEmptyFilterParams()
	}

	criteria := make([]string, 0)

	id, idOk := filter.GetAsNullableString("id")
	if idOk {
		criteria = append(criteria, "id='"+id+"'")
	}

	tempIds, idsOk := filter.GetAsNullableString("ids")
	if idsOk {
		ids := strings.Split(tempIds, ",")
		criteria = append(criteria, "id IN ('"+strings.Join(ids, "','")+"')")
	}

	key, keyOk := filter.GetAsNullableString("key")
	if keyOk {
		criteria = append(criteria, "key='"+key+"'")
	}

	if len(criteria) > 0 {
		return strings.Join(criteria, " AND ")
	} else {
		return ""
	}
}

func (c *MyPostgresPersistence) GetPageByFilter(ctx context.Context, correlationId string, filter data.FilterParams, paging data.PagingParams) (page data.DataPage[MyData], err error) {
	return c.IdentifiablePostgresPersistence.GetPageByFilter(ctx, correlationId, c.composeFilter(filter), paging, "id", "")
}

func (c *MyPostgresPersistence) GetOneByKey(ctx context.Context, correlationId string, key string) (item MyData, err error) {
	query := "SELECT * FROM " + c.QuotedTableName() + " WHERE \"key\"=$1"

	rows, err := c.Client.Query(ctx, query, key)
	if err != nil {
		return item, err
	}
	defer rows.Close()

	if !rows.Next() {
		return item, rows.Err()
	}

	values, err := rows.Values()
	if err == nil && len(values) > 0 {
		c.Logger.Trace(ctx, correlationId, "Retrieved from %s with key = %s", c.TableName, key)
		return c.Overrides.ConvertToPublic(rows)
	}
	c.Logger.Trace(ctx, correlationId, "Nothing found from %s with key = %s", c.TableName, key)
	return item, err
}

```

Alternatively you can store data in non-relational format using `IdentificableJsonPostgresPersistence`.
It stores data in tables with two columns - `id` with unique object id and `data` with object data serialized as JSON.
To access data fields you shall use `data->'field'` expression or `data->>'field'` expression for string values.

```go
type MyPostgresPersistence struct {
	*persistence.IdentifiableJsonPostgresPersistence[MyData, string]
}

func NewMyPostgresPersistence() *MyPostgresPersistence {
	c := &MyPostgresPersistence{}
	c.IdentifiableJsonPostgresPersistence = persistence.InheritIdentifiableJsonPostgresPersistence[MyData, string](c, "my_data_json")
	return c
}

func (c *MyPostgresPersistence) DefineSchema() {
	c.ClearSchema()
	c.IdentifiableJsonPostgresPersistence.DefineSchema()
	c.EnsureTable("VARCHAR(32)", "JSONB")
	c.EnsureIndex(c.TableName+"_key", map[string]string{"(data->'key')": "1"}, map[string]string{"unique": "true"})
}

func (c *MyPostgresPersistence) composeFilter(filter data.FilterParams) string {
	criteria := make([]string, 0)

	id, idOk := filter.GetAsNullableString("id")
	if idOk {
		criteria = append(criteria, "data->>'id'='"+id+"'")
	}

	tempIds, idsOk := filter.GetAsNullableString("ids")
	if idsOk {
		ids := strings.Split(tempIds, ",")
		criteria = append(criteria, "data->>'id' IN ('"+strings.Join(ids, "','")+"')")
	}

	key, keyOk := filter.GetAsNullableString("key")
	if keyOk {
		criteria = append(criteria, "data->>'key'='"+key+"'")
	}

	if len(criteria) > 0 {
		return strings.Join(criteria, " AND ")
	} else {
		return ""
	}
}

func (c *MyPostgresPersistence) GetPageByFilter(ctx context.Context, correlationId string, filter data.FilterParams, paging data.PagingParams) (page data.DataPage[MyData], err error) {
	return c.IdentifiablePostgresPersistence.GetPageByFilter(ctx, correlationId, c.composeFilter(filter), paging, "id", "")
}

func (c *MyPostgresPersistence) GetOneByKey(ctx context.Context, correlationId string, key string) (item MyData, err error) {
	query := "SELECT * FROM " + c.QuotedTableName() + " WHERE data->>'key'=$1"

	rows, err := c.Client.Query(ctx, query, key)
	if err != nil {
		return item, err
	}
	defer rows.Close()

	if !rows.Next() {
		return item, rows.Err()
	}

	values, err := rows.Values()
	if err == nil && len(values) > 0 {
		c.Logger.Trace(ctx, correlationId, "Retrieved from %s with key = %s", c.TableName, key)
		return c.Overrides.ConvertToPublic(rows)
	}
	c.Logger.Trace(ctx, correlationId, "Nothing found from %s with key = %s", c.TableName, key)
	return item, err
}

```

Configuration for your microservice that includes postgresql persistence may look the following way.

```yml
...
{{#if POSTGRES_ENABLED}}
- descriptor: pip-services:connection:postgres:con1:1.0
  connection:
    uri: {{{POSTGRES_SERVICE_URI}}}
    host: {{{POSTGRES_SERVICE_HOST}}}{{#unless POSTGRES_SERVICE_HOST}}localhost{{/unless}}
    port: {{POSTGRES_SERVICE_PORT}}{{#unless POSTGRES_SERVICE_PORT}}5432{{/unless}}
    database: {{POSTGRES_DB}}{{#unless POSTGRES_DB}}app{{/unless}}
  credential:
    username: {{POSTGRES_USER}}
    password: {{POSTGRES_PASS}}
    
- descriptor: myservice:persistence:postgres:default:1.0
  dependencies:
    connection: pip-services:connection:postgres:con1:1.0
  table: {{POSTGRES_TABLE}}{{#unless POSTGRES_TABLE}}myobjects{{/unless}}
{{/if}}
...
```

## Develop

For development you shall install the following prerequisites:
* Golang v1.18+
* Visual Studio Code or another IDE of your choice
* Docker
* Git

Run automated tests:
```bash
go test -v ./test/...
```

Generate API documentation:
```bash
./docgen.ps1
```

Before committing changes run dockerized test as:
```bash
./test.ps1
./clear.ps1
```

## Contacts

The library is created and maintained by **Sergey Seroukhov**, **Dmitrii Uzdemir** and **Dmitrii Levichev**.

The documentation is written by **Mark Makarychev**.
