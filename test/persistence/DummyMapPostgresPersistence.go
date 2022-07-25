package test

import (
	"context"
	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	persist "github.com/pip-services3-gox/pip-services3-postgres-gox/persistence"
)

type DummyMapPostgresPersistence struct {
	persist.IdentifiablePostgresPersistence[map[string]any, string]
}

func NewDummyMapPostgresPersistence() *DummyMapPostgresPersistence {
	c := &DummyMapPostgresPersistence{}
	c.IdentifiablePostgresPersistence = *persist.InheritIdentifiablePostgresPersistence[map[string]any, string](c, "dummies")
	return c
}

func (c *DummyMapPostgresPersistence) DefineSchema() {
	c.ClearSchema()
	c.IdentifiablePostgresPersistence.DefineSchema()
	c.EnsureSchema("CREATE TABLE \"" + c.IdentifiablePostgresPersistence.TableName + "\" (\"id\" TEXT PRIMARY KEY, \"key\" TEXT, \"content\" TEXT)")
	c.EnsureIndex(c.IdentifiablePostgresPersistence.TableName+"_key", map[string]string{"key": "1"}, map[string]string{"unique": "true"})
}

func (c *DummyMapPostgresPersistence) GetPageByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[map[string]any], err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "key='" + key + "'"
	}
	sorting := ""

	return c.IdentifiablePostgresPersistence.GetPageByFilter(ctx, correlationId,
		filterObj, paging, sorting, "",
	)
}

func (c *DummyMapPostgresPersistence) GetCountByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams) (count int64, err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "key='" + key + "'"
	}
	return c.IdentifiablePostgresPersistence.GetCountByFilter(ctx, correlationId, filterObj)
}
