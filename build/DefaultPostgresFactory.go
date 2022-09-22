package build

import (
	cref "github.com/pip-services3-gox/pip-services3-commons-gox/refer"
	cbuild "github.com/pip-services3-gox/pip-services3-components-gox/build"
	conn "github.com/pip-services3-gox/pip-services3-postgres-gox/connect"
)

// DefaultPostgresFactory creates Postgres components by their descriptors.
//	see Factory
//	see PostgresConnection
type DefaultPostgresFactory struct {
	*cbuild.Factory
}

//	Create a new instance of the factory.
func NewDefaultPostgresFactory() *DefaultPostgresFactory {

	c := &DefaultPostgresFactory{}
	c.Factory = cbuild.NewFactory()

	postgresConnectionDescriptor := cref.NewDescriptor("pip-services", "connection", "postgres", "*", "1.0")
	c.RegisterType(postgresConnectionDescriptor, conn.NewPostgresConnection)

	return c
}
