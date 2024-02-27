package operationreport

type PerformanceReport struct {
	IsUsingCachedPlan    bool
	SchemaValidationTime int64
	PlanTime             int64
	ResolveResponseTime  int64
}
