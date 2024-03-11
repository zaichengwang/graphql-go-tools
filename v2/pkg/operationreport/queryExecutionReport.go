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

	// below is the metric for planning path, the key is the path, the value is the corresponding datasource ID
	PlanDecisionMap   map[string]string
	PlanDecisionError error
}
