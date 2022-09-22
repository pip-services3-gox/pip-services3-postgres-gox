package test

import (
	"context"

	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	persist "github.com/pip-services3-gox/pip-services3-postgres-gox/persistence"
	"github.com/pip-services3-gox/pip-services3-postgres-gox/test/fixtures"
)

type DummyJsonPostgresPersistence struct {
	*persist.IdentifiableJsonPostgresPersistence[fixtures.Dummy, string]
}

func NewDummyJsonPostgresPersistence() *DummyJsonPostgresPersistence {
	c := &DummyJsonPostgresPersistence{}
	c.IdentifiableJsonPostgresPersistence = persist.InheritIdentifiableJsonPostgresPersistence[fixtures.Dummy, string](c, "dummies_json")
	return c
}

func (c *DummyJsonPostgresPersistence) DefineSchema() {
	c.ClearSchema()
	c.IdentifiableJsonPostgresPersistence.DefineSchema()
	c.EnsureTable("", "")
	c.EnsureIndex(c.TableName+"_key", map[string]string{"(data->'key')": "1"}, map[string]string{"unique": "true"})
}

func (c *DummyJsonPostgresPersistence) GetPageByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[fixtures.Dummy], err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "data->>'key'='" + key + "'"
	}

	return c.IdentifiableJsonPostgresPersistence.GetPageByFilter(ctx, correlationId,
		filterObj, paging,
		"", "",
	)
}

func (c *DummyJsonPostgresPersistence) GetCountByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams) (count int64, err error) {

	filterObj := ""
	if key, ok := filter.GetAsNullableString("Key"); ok && key != "" {
		filterObj += "data->>'key'='" + key + "'"
	}

	return c.IdentifiableJsonPostgresPersistence.GetCountByFilter(ctx, correlationId, filterObj)
}

func (c *DummyJsonPostgresPersistence) GetOneRandom(ctx context.Context, correlationId string) (item fixtures.Dummy, err error) {
	return c.IdentifiableJsonPostgresPersistence.GetOneRandom(ctx, correlationId, "")
}
