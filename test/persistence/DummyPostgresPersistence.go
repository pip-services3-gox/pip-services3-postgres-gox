package test

import (
	"context"
	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	persist "github.com/pip-services3-gox/pip-services3-postgres-gox/persistence"
	"github.com/pip-services3-gox/pip-services3-postgres-gox/test/fixtures"
)

type DummyPostgresPersistence struct {
	persist.IdentifiablePostgresPersistence[fixtures.Dummy, string]
}

func NewDummyPostgresPersistence() *DummyPostgresPersistence {
	c := &DummyPostgresPersistence{}
	c.IdentifiablePostgresPersistence = *persist.InheritIdentifiablePostgresPersistence[fixtures.Dummy, string](c, "dummies")
	return c
}

func (c *DummyPostgresPersistence) DefineSchema() {
	c.ClearSchema()
	c.IdentifiablePostgresPersistence.DefineSchema()
	// Row name must be in double quotes for properly case!!!
	c.EnsureSchema("CREATE TABLE " + c.QuotedTableName() + " (\"id\" TEXT PRIMARY KEY, \"key\" TEXT, \"content\" TEXT)")
	c.EnsureIndex(c.IdentifiablePostgresPersistence.TableName+"_key", map[string]string{"key": "1"}, map[string]string{"unique": "true"})
}

func (c *DummyPostgresPersistence) GetPageByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[fixtures.Dummy], err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "key='" + key + "'"
	}
	sorting := ""

	return c.IdentifiablePostgresPersistence.GetPageByFilter(ctx, correlationId,
		filterObj, paging,
		sorting, nil,
	)
}

func (c *DummyPostgresPersistence) GetCountByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams) (count int64, err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "key='" + key + "'"
	}
	return c.IdentifiablePostgresPersistence.GetCountByFilter(ctx, correlationId, filterObj)
}

func (c *DummyPostgresPersistence) GetOneRandom(ctx context.Context, correlationId string) (item fixtures.Dummy, err error) {
	return c.IdentifiablePostgresPersistence.GetOneRandom(ctx, correlationId, nil)
}
