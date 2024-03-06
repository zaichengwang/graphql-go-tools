package operationreport

type QueryExecutionReport struct {
	IsUsingCachedPlan    bool
	SchemaValidationTime int64
	PlanTime             int64
	ResolveResponseTime  int64

	SchemaNormalizationError     error
	SchemaValidationError        error
	QueryPlanningError           error
	LoadGraphQLResponseDataError error
	ResolveGraphQLResponseError  error
}
