package test

import (
	"context"
	cdata "github.com/pip-services3-gox/pip-services3-commons-gox/data"
	persist "github.com/pip-services3-gox/pip-services3-postgres-gox/persistence"
	"github.com/pip-services3-gox/pip-services3-postgres-gox/test/fixtures"
)

type DummyRefPostgresPersistence struct {
	persist.IdentifiablePostgresPersistence[*fixtures.Dummy, string]
}

func NewDummyRefPostgresPersistence() *DummyRefPostgresPersistence {
	c := &DummyRefPostgresPersistence{}
	c.IdentifiablePostgresPersistence = *persist.InheritIdentifiablePostgresPersistence[*fixtures.Dummy, string](c, "dummies")
	return c
}

func (c *DummyRefPostgresPersistence) GetPageByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams, paging cdata.PagingParams) (page cdata.DataPage[*fixtures.Dummy], err error) {

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

func (c *DummyRefPostgresPersistence) GetCountByFilter(ctx context.Context, correlationId string,
	filter cdata.FilterParams) (count int64, err error) {

	key, ok := filter.GetAsNullableString("Key")
	filterObj := ""
	if ok && key != "" {
		filterObj += "key='" + key + "'"
	}
	return c.IdentifiablePostgresPersistence.GetCountByFilter(ctx, correlationId, filterObj)
}
