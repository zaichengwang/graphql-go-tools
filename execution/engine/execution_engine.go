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

type internalExecutionContext struct {
	resolveContext *resolve.Context
	postProcessor  *postprocess.Processor
}

func newInternalExecutionContext() *internalExecutionContext {
	return &internalExecutionContext{
		resolveContext: resolve.NewContext(context.Background()),
		postProcessor:  postprocess.DefaultProcessor(),
	}
}

func (e *internalExecutionContext) prepare(ctx context.Context, variables []byte, request resolve.Request, options ...ExecutionOptions) {
	e.setContext(ctx)
	e.setVariables(variables)
	e.setRequest(request)
}

func (e *internalExecutionContext) setRequest(request resolve.Request) {
	e.resolveContext.Request = request
}

func (e *internalExecutionContext) setContext(ctx context.Context) {
	e.resolveContext = e.resolveContext.WithContext(ctx)
}

func (e *internalExecutionContext) setVariables(variables []byte) {
	e.resolveContext.Variables = variables
}

func (e *internalExecutionContext) reset() {
	e.resolveContext.Free()
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

type ExecutionOptions func(ctx *internalExecutionContext)

func WithAdditionalHttpHeaders(headers http.Header, excludeByKeys ...string) ExecutionOptions {
	return func(ctx *internalExecutionContext) {
		if len(headers) == 0 {
			return
		}

		if ctx.resolveContext.Request.Header == nil {
			ctx.resolveContext.Request.Header = make(http.Header)
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
				ctx.resolveContext.Request.Header.Add(headerKey, headerValue)
			}
		}
	}
}

func WithRequestTraceOptions(options resolve.TraceOptions) ExecutionOptions {
	return func(ctx *internalExecutionContext) {
		ctx.resolveContext.TracingOptions = options
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

	execContext := e.prepareExecutionContext(ctx, operation, options...)

	traceTimings := resolve.NewTraceTimings(execContext.resolveContext.Context())

	if err := e.normalizeOperation(operation, execContext, traceTimings); err != nil {
		return err, resolve.GetTraceInfo(execContext.resolveContext.Context())
	}

	if err := e.validateOperation(operation, execContext, traceTimings); err != nil {:
		return err, resolve.GetTraceInfo(execContext.resolveContext.Context())
	}

	cachedPlan, err := e.planOperation(operation, execContext, traceTimings)
	if err != nil {
		return err, resolve.GetTraceInfo(execContext.resolveContext.Context())
	}

	err = e.executePlan(execContext, writer, traceTimings, cachedPlan)
	if err != nil {
		return err, resolve.GetTraceInfo(execContext.resolveContext.Context())
	}

	return nil, resolve.GetTraceInfo(execContext.resolveContext.Context())
}

func (e *ExecutionEngine) getCachedPlan(ctx *internalExecutionContext, operation, definition *ast.Document, operationName string, report *operationreport.Report) plan.Plan {

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

func suggestionsToPlanningStats(planResult plan.Plan) resolve.PlanningPathStats {
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

func (e *ExecutionEngine) prepareExecutionContext(ctx context.Context, operation *graphql.Request, options ...ExecutionOptions) *internalExecutionContext {
	execContext := newInternalExecutionContext()
	execContext.setContext(ctx)

	for i := range options {
		options[i](execContext)
	}

	execContext.prepare(ctx, operation.Variables, operation.InternalRequest(), options...)

	if execContext.resolveContext.TracingOptions.Enable {
		traceCtx := resolve.SetTraceStart(execContext.resolveContext.Context(), execContext.resolveContext.TracingOptions.EnablePredictableDebugTimings)
		execContext.setContext(traceCtx)
	}

	return execContext
}

func (e *ExecutionEngine) normalizeOperation(operation *graphql.Request, execContext *internalExecutionContext, traceTimings *resolve.TraceTimings) error {
	traceTimings.StartNormalize()
	defer traceTimings.EndNormalize()

	if !operation.IsNormalized() {
		result, err := operation.Normalize(e.config.schema)
		if err != nil {
			resolve.SetRequestTracingStats(execContext.resolveContext.Context(), execContext.resolveContext.TracingOptions, traceTimings)
			return err
		}

		if !result.Successful {
			resolve.SetRequestTracingStats(execContext.resolveContext.Context(), execContext.resolveContext.TracingOptions, traceTimings)
			return result.Errors
		}
	}

	return nil
}

func (e *ExecutionEngine) validateOperation(operation *graphql.Request, execContext *internalExecutionContext, traceTimings *resolve.TraceTimings) error {
	traceTimings.StartValidate()
	defer traceTimings.EndValidate()

	result, err := operation.ValidateForSchema(e.config.schema)
	if err != nil {
		resolve.SetRequestTracingStats(execContext.resolveContext.Context(), execContext.resolveContext.TracingOptions, traceTimings)
		return err
	}
	if !result.Valid {
		resolve.SetRequestTracingStats(execContext.resolveContext.Context(), execContext.resolveContext.TracingOptions, traceTimings)
		return result.Errors
	}

	return nil
}

func (e *ExecutionEngine) planOperation(operation *graphql.Request, execContext *internalExecutionContext, traceTimings *resolve.TraceTimings) (interface{}, error) {
	traceTimings.StartPlanning()
	defer traceTimings.EndPlanning()

	var report operationreport.Report
	cachedPlan := e.getCachedPlan(execContext, operation.Document(), e.config.schema.Document(), operation.OperationName, &report)
	if report.HasErrors() {
		resolve.SetPlanningTracingStat(execContext.resolveContext.Context(), execContext.resolveContext.TracingOptions, traceTimings, suggestionsToPlanningStats(cachedPlan))
		return nil, report
	}

	resolve.SetPlanningTracingStat(execContext.resolveContext.Context(), execContext.resolveContext.TracingOptions, traceTimings, suggestionsToPlanningStats(cachedPlan))
	return cachedPlan, nil
}

func (e *ExecutionEngine) executePlan(execContext *internalExecutionContext, writer resolve.SubscriptionResponseWriter, traceTimings *resolve.TraceTimings, cachedPlan interface{}) error {
	switch p := cachedPlan.(type) {
	case *plan.SynchronousResponsePlan:
		return e.resolver.ResolveGraphQLResponse(execContext.resolveContext, p.Response, nil, writer, traceTimings)
	case *plan.SubscriptionResponsePlan:
		return e.resolver.ResolveGraphQLSubscription(execContext.resolveContext, p.Response, writer)
	default:
		return errors.New("execution of operation is not possible")
	}
}
