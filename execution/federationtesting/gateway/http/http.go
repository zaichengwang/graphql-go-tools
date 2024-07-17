// Package http handles GraphQL HTTP Requests including WebSocket Upgrades.
package http

import (
	"bytes"
	"context"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/operationreport"
	"net/http"

	log "github.com/jensneuse/abstractlogger"

	"github.com/wundergraph/graphql-go-tools/execution/engine"
	"github.com/wundergraph/graphql-go-tools/execution/graphql"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"
)

const (
	httpHeaderContentType          string = "Content-Type"
	httpContentTypeApplicationJson string = "application/json"
)

func (g *GraphQLHTTPRequestHandler) handleHTTP(w http.ResponseWriter, r *http.Request) {
	var err error

	var gqlRequest graphql.Request
	if err = graphql.UnmarshalHttpRequest(r, &gqlRequest); err != nil {
		g.log.Error("UnmarshalHttpRequest", log.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var opts []engine.ExecutionOptions

	if g.enableART {
		tracingOpts := resolve.TraceOptions{
			Enable:                                 true,
			ExcludePlannerStats:                    false,
			ExcludeRawInputData:                    false,
			ExcludeInput:                           false,
			ExcludeOutput:                          false,
			ExcludeLoadStats:                       false,
			EnablePredictableDebugTimings:          false,
			IncludeTraceOutputInResponseExtensions: true,
		}

		opts = append(opts, engine.WithRequestTraceOptions(tracingOpts))
	}

	buf := bytes.NewBuffer(make([]byte, 0, 4096))
	resultWriter := graphql.NewEngineResultWriterFromBuffer(buf)
	if err, _ = execute(g.engine, r.Context(), &gqlRequest, &resultWriter, opts...); err != nil {
		g.log.Error("engine.Execute", log.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Add(httpHeaderContentType, httpContentTypeApplicationJson)
	w.WriteHeader(http.StatusOK)
	if _, err = w.Write(buf.Bytes()); err != nil {
		g.log.Error("write response", log.Error(err))
		return
	}
}

func execute(e *engine.ExecutionEngine, ctx context.Context, operation *graphql.Request, writer resolve.SubscriptionResponseWriter, options ...engine.ExecutionOptions) (error, *resolve.TraceInfo) {

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

	resolve.SetPlanningTracingStat(execContext.ResolveContext.Context(), execContext.ResolveContext.TracingOptions, traceTimings, engine.SuggestionsToPlanningStats(cachedPlan))
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
