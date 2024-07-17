package engine

import (
	"context"
	"errors"
	lru "github.com/hashicorp/golang-lru"
	"github.com/jensneuse/abstractlogger"
	"net/http"
	"sync"

	"github.com/wundergraph/graphql-go-tools/execution/graphql"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/astprinter"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/datasource/introspection_datasource"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/plan"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/postprocess"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/operationreport"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/pool"
)

type InternalExecutionContext struct {
	ResolveContext *resolve.Context
	postProcessor  *postprocess.Processor
}

func newInternalExecutionContext() *InternalExecutionContext {
	return &InternalExecutionContext{
		ResolveContext: resolve.NewContext(context.Background()),
		postProcessor:  postprocess.DefaultProcessor(),
	}
}

func (e *InternalExecutionContext) Prepare(ctx context.Context, variables []byte, request resolve.Request, options ...ExecutionOptions) {
	e.setContext(ctx)
	e.setVariables(variables)
	e.setRequest(request)
}

func (e *InternalExecutionContext) setRequest(request resolve.Request) {
	e.ResolveContext.Request = request
}

func (e *InternalExecutionContext) setContext(ctx context.Context) {
	e.ResolveContext = e.ResolveContext.WithContext(ctx)
}

func (e *InternalExecutionContext) setVariables(variables []byte) {
	e.ResolveContext.Variables = variables
}

func (e *InternalExecutionContext) reset() {
	e.ResolveContext.Free()
}

type ExecutionEngine struct {
	logger             abstractlogger.Logger
	config             Configuration
	planner            *plan.Planner
	plannerMu          sync.Mutex
	resolver           *resolve.Resolver
	executionPlanCache *lru.Cache
}

type WebsocketBeforeStartHook interface {
	OnBeforeStart(reqCtx context.Context, operation *graphql.Request) error
}

type ExecutionOptions func(ctx *InternalExecutionContext)

func WithAdditionalHttpHeaders(headers http.Header, excludeByKeys ...string) ExecutionOptions {
	return func(ctx *InternalExecutionContext) {
		if len(headers) == 0 {
			return
		}

		if ctx.ResolveContext.Request.Header == nil {
			ctx.ResolveContext.Request.Header = make(http.Header)
		}

		excludeMap := make(map[string]bool)
		for _, key := range excludeByKeys {
			excludeMap[key] = true
		}

		for headerKey, headerValues := range headers {
			if excludeMap[headerKey] {
				continue
			}

			for _, headerValue := range headerValues {
				ctx.ResolveContext.Request.Header.Add(headerKey, headerValue)
			}
		}
	}
}

func WithRequestTraceOptions(options resolve.TraceOptions) ExecutionOptions {
	return func(ctx *InternalExecutionContext) {
		ctx.ResolveContext.TracingOptions = options
	}
}

func NewExecutionEngine(ctx context.Context, logger abstractlogger.Logger, engineConfig Configuration, resolverOptions resolve.ResolverOptions) (*ExecutionEngine, error) {
	executionPlanCache, err := lru.New(1024)
	if err != nil {
		return nil, err
	}

	introspectionCfg, err := introspection_datasource.NewIntrospectionConfigFactory(engineConfig.schema.Document())
	if err != nil {
		return nil, err
	}

	for _, dataSource := range introspectionCfg.BuildDataSourceConfigurations() {
		engineConfig.AddDataSource(dataSource)
	}

	for _, fieldCfg := range introspectionCfg.BuildFieldConfigurations() {
		engineConfig.AddFieldConfiguration(fieldCfg)
	}

	planner, err := plan.NewPlanner(engineConfig.plannerConfig)
	if err != nil {
		return nil, err
	}

	return &ExecutionEngine{
		logger:             logger,
		config:             engineConfig,
		planner:            planner,
		resolver:           resolve.New(ctx, resolverOptions),
		executionPlanCache: executionPlanCache,
	}, nil
}

func (e *ExecutionEngine) Execute(ctx context.Context, operation *graphql.Request, writer resolve.SubscriptionResponseWriter, options ...ExecutionOptions) (error, *resolve.TraceInfo) {

	execContext := e.PrepareExecutionContext(ctx, operation, options...)
	traceTimings := resolve.NewTraceTimings(execContext.ResolveContext.Context())

	if err := e.NormalizeOperation(operation, execContext, traceTimings); err != nil {
		return err, resolve.GetTraceInfo(execContext.ResolveContext.Context())
	}

	if err := e.ValidateOperation(operation, execContext, traceTimings); err != nil {
		return err, resolve.GetTraceInfo(execContext.ResolveContext.Context())
	}

	execContext.Prepare(execContext.ResolveContext.Context(), operation.Variables, operation.InternalRequest(), options...)
	var report operationreport.Report

	cachedPlan, err := e.PlanOperation(operation, execContext, traceTimings, report)
	if err != nil {
		return err, resolve.GetTraceInfo(execContext.ResolveContext.Context())
	}

	resolve.SetPlanningTracingStat(execContext.ResolveContext.Context(), execContext.ResolveContext.TracingOptions, traceTimings, SuggestionsToPlanningStats(cachedPlan))
	err = e.ExecutePlan(execContext, writer, traceTimings, cachedPlan)
	if err != nil {
		return err, resolve.GetTraceInfo(execContext.ResolveContext.Context())
	}

	if execContext.ResolveContext != nil && execContext.ResolveContext.Trace.Fetch != nil && execContext.ResolveContext.Trace.Fetch.DataSourceLoadTrace != nil {
		resolve.SetHttpCallData(execContext.ResolveContext.Context(),
			execContext.ResolveContext.Trace.Fetch.DataSourceLoadTrace.HttpCallData)
	}

	return nil, resolve.GetTraceInfo(execContext.ResolveContext.Context())
}

func (e *ExecutionEngine) getCachedPlan(ctx *InternalExecutionContext, operation, definition *ast.Document, operationName string, report *operationreport.Report) plan.Plan {

	hash := pool.Hash64.Get()
	hash.Reset()
	defer pool.Hash64.Put(hash)
	err := astprinter.Print(operation, definition, hash)
	if err != nil {
		report.AddInternalError(err)
		return nil
	}

	cacheKey := hash.Sum64()

	if cached, ok := e.executionPlanCache.Get(cacheKey); ok {
		if p, ok := cached.(plan.Plan); ok {
			return p
		}
	}

	e.plannerMu.Lock()
	defer e.plannerMu.Unlock()
	planResult := e.planner.Plan(operation, definition, operationName, report)
	if report.HasErrors() {
		return nil
	}

	p := ctx.postProcessor.Process(planResult)
	e.executionPlanCache.Add(cacheKey, p)
	return p
}

func (e *ExecutionEngine) GetWebsocketBeforeStartHook() WebsocketBeforeStartHook {
	return e.config.websocketBeforeStartHook
}

func SuggestionsToPlanningStats(planResult plan.Plan) resolve.PlanningPathStats {
	planningPath := make(map[string]string)

	if planResult == nil || planResult.NodeSuggestions() == nil {
		return resolve.PlanningPathStats{
			PlanningPath: planningPath,
		}
	}
	suggestions := planResult.NodeSuggestions()

	for _, suggestion := range suggestions {
		if suggestion.Selected && suggestion.IsRootNode && (suggestion.TypeName == "Query" || suggestion.TypeName == "Mutation") {
			planningPath[suggestion.Path] = suggestion.DataSourceID
		}
	}
	return resolve.PlanningPathStats{
		PlanningPath: planningPath,
	}
}

func (e *ExecutionEngine) PrepareExecutionContext(ctx context.Context, operation *graphql.Request, options ...ExecutionOptions) *InternalExecutionContext {
	execContext := newInternalExecutionContext()
	execContext.setContext(ctx)

	for i := range options {
		options[i](execContext)
	}

	execContext.Prepare(ctx, operation.Variables, operation.InternalRequest(), options...)

	if execContext.ResolveContext.TracingOptions.Enable {
		traceCtx := resolve.SetTraceStart(execContext.ResolveContext.Context(), execContext.ResolveContext.TracingOptions.EnablePredictableDebugTimings)
		execContext.setContext(traceCtx)
	}

	return execContext
}

func (e *ExecutionEngine) NormalizeOperation(operation *graphql.Request, execContext *InternalExecutionContext, traceTimings *resolve.TraceTimings) error {
	traceTimings.StartNormalize()
	defer traceTimings.EndNormalize()

	if !operation.IsNormalized() {
		result, err := operation.Normalize(e.config.schema)
		if err != nil {
			resolve.SetRequestTracingStats(execContext.ResolveContext.Context(), execContext.ResolveContext.TracingOptions, traceTimings)
			return err
		}

		if !result.Successful {
			resolve.SetRequestTracingStats(execContext.ResolveContext.Context(), execContext.ResolveContext.TracingOptions, traceTimings)
			return result.Errors
		}
	}

	return nil
}

func (e *ExecutionEngine) ValidateOperation(operation *graphql.Request, execContext *InternalExecutionContext, traceTimings *resolve.TraceTimings) error {
	traceTimings.StartValidate()
	defer traceTimings.EndValidate()

	result, err := operation.ValidateForSchema(e.config.schema)
	if err != nil {
		resolve.SetRequestTracingStats(execContext.ResolveContext.Context(), execContext.ResolveContext.TracingOptions, traceTimings)
		return err
	}
	if !result.Valid {
		resolve.SetRequestTracingStats(execContext.ResolveContext.Context(), execContext.ResolveContext.TracingOptions, traceTimings)
		return result.Errors
	}

	return nil
}

func (e *ExecutionEngine) PlanOperation(operation *graphql.Request, execContext *InternalExecutionContext, traceTimings *resolve.TraceTimings, report operationreport.Report) (plan.Plan, error) {
	traceTimings.StartPlanning()
	defer traceTimings.EndPlanning()

	cachedPlan := e.getCachedPlan(execContext, operation.Document(), e.config.schema.Document(), operation.OperationName, &report)
	if report.HasErrors() {
		resolve.SetPlanningTracingStat(execContext.ResolveContext.Context(), execContext.ResolveContext.TracingOptions, traceTimings, SuggestionsToPlanningStats(cachedPlan))
		return nil, report
	}

	resolve.SetPlanningTracingStat(execContext.ResolveContext.Context(), execContext.ResolveContext.TracingOptions, traceTimings, SuggestionsToPlanningStats(cachedPlan))
	return cachedPlan, nil
}

func (e *ExecutionEngine) ExecutePlan(execContext *InternalExecutionContext, writer resolve.SubscriptionResponseWriter, traceTimings *resolve.TraceTimings, cachedPlan interface{}) error {
	switch p := cachedPlan.(type) {
	case *plan.SynchronousResponsePlan:
		return e.resolver.ResolveGraphQLResponse(execContext.ResolveContext, p.Response, nil, writer, traceTimings)
	case *plan.SubscriptionResponsePlan:
		return e.resolver.ResolveGraphQLSubscription(execContext.ResolveContext, p.Response, writer)
	default:
		return errors.New("execution of operation is not possible")
	}
}
