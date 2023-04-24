package drivers

type Driver interface {
	ExecuteQuery(entity string, selectFields string, filterFields string) (string, error)
}
