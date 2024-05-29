package resolve

import (
	"context"
	"time"
)

type TraceTimings struct {
	ctx            context.Context
	ParseStart     int64
	ParseEnd       int64
	NormalizeStart int64
	NormalizeEnd   int64
	ValidateStart  int64
	ValidateEnd    int64
	PlanningStart  int64
	PlanningEnd    int64

	LoadResponseStart int64
	LoadResponseEnd   int64
	ResolveStart      int64
	ResolveEnd        int64
}

func NewTraceTimings(ctx context.Context) *TraceTimings {
	return &TraceTimings{
		ctx: ctx,
	}
}

func (tt *TraceTimings) StartResolve() {
	tt.ResolveStart = GetDurationNanoSinceTraceStart(tt.ctx)
}

func (tt *TraceTimings) EndResolve() {
	tt.ResolveEnd = GetDurationNanoSinceTraceStart(tt.ctx)
}

func (tt *TraceTimings) StartLoadResponse() {
	tt.LoadResponseStart = GetDurationNanoSinceTraceStart(tt.ctx)
}

func (tt *TraceTimings) EndLoadResponse() {
	tt.LoadResponseEnd = GetDurationNanoSinceTraceStart(tt.ctx)
}

func (tt *TraceTimings) StartParse() {
	tt.ParseStart = GetDurationNanoSinceTraceStart(tt.ctx)
}

func (tt *TraceTimings) EndParse() {
	tt.ParseEnd = GetDurationNanoSinceTraceStart(tt.ctx)
}

// StartNormalize starts the timing for the normalization step
func (tt *TraceTimings) StartNormalize() {
	tt.NormalizeStart = GetDurationNanoSinceTraceStart(tt.ctx)
}

func (tt *TraceTimings) EndNormalize() {
	tt.NormalizeEnd = GetDurationNanoSinceTraceStart(tt.ctx)
}

func (tt *TraceTimings) StartValidate() {
	tt.ValidateStart = GetDurationNanoSinceTraceStart(tt.ctx)
}

func (tt *TraceTimings) EndValidate() {
	tt.ValidateEnd = GetDurationNanoSinceTraceStart(tt.ctx)
}

func (tt *TraceTimings) StartPlanning() {
	tt.PlanningStart = GetDurationNanoSinceTraceStart(tt.ctx)
}

func (tt *TraceTimings) EndPlanning() {
	tt.PlanningEnd = GetDurationNanoSinceTraceStart(tt.ctx)
}

func (tt *TraceTimings) DurationParse() int64 {
	return tt.ParseEnd - tt.ParseStart
}

func (tt *TraceTimings) DurationNormalize() int64 {
	return tt.NormalizeEnd - tt.NormalizeStart
}

func (tt *TraceTimings) DurationValidate() int64 {
	return tt.ValidateEnd - tt.ValidateStart
}

func (tt *TraceTimings) DurationPlanning() int64 {
	return tt.PlanningEnd - tt.PlanningStart
}

func SetRequestTracingStats(ctx context.Context, traceOptions TraceOptions, traceTimings *TraceTimings) {
	if !traceOptions.ExcludeParseStats {
		SetParseStats(ctx, PhaseStats{
			DurationSinceStartNano:   traceTimings.ParseStart,
			DurationSinceStartPretty: time.Duration(traceTimings.ParseStart).String(),
			DurationNano:             traceTimings.DurationParse(),
			DurationPretty:           time.Duration(traceTimings.DurationParse()).String(),
		})
	}
	if !traceOptions.ExcludeNormalizeStats {
		SetNormalizeStats(ctx, PhaseStats{
			DurationSinceStartNano:   traceTimings.NormalizeStart,
			DurationSinceStartPretty: time.Duration(traceTimings.NormalizeStart).String(),
			DurationNano:             traceTimings.DurationNormalize(),
			DurationPretty:           time.Duration(traceTimings.DurationNormalize()).String(),
		})
	}
	if !traceOptions.ExcludeValidateStats {
		SetValidateStats(ctx, PhaseStats{
			DurationSinceStartNano:   traceTimings.ValidateStart,
			DurationSinceStartPretty: time.Duration(traceTimings.ValidateStart).String(),
			DurationNano:             traceTimings.DurationValidate(),
			DurationPretty:           time.Duration(traceTimings.DurationValidate()).String(),
		})
	}
}

func SetPlanningTracingStat(ctx context.Context, traceOptions TraceOptions, traceTimings *TraceTimings, pathStats PlanningPathStats) {
	SetPlannerStats(ctx, PhaseStats{
		DurationSinceStartNano:   traceTimings.PlanningStart,
		DurationSinceStartPretty: time.Duration(traceTimings.PlanningStart).String(),
		DurationNano:             traceTimings.DurationPlanning(),
		DurationPretty:           time.Duration(traceTimings.DurationPlanning()).String(),
	}, pathStats)
}

func SetResolveTracingStat(ctx context.Context, traceOptions TraceOptions, traceTimings *TraceTimings) {
	SetResolveStats(ctx, PhaseStats{
		DurationSinceStartNano:   traceTimings.ResolveStart,
		DurationSinceStartPretty: time.Duration(traceTimings.ResolveStart).String(),
		DurationNano:             traceTimings.ResolveEnd - traceTimings.ResolveStart,
		DurationPretty:           time.Duration(traceTimings.ResolveEnd - traceTimings.ResolveStart).String(),
	})
}

func SetLoadResponseTracingStat(ctx context.Context, traceOptions TraceOptions, traceTimings *TraceTimings) {
	SetLoadResponseStats(ctx, PhaseStats{
		DurationSinceStartNano:   traceTimings.LoadResponseStart,
		DurationSinceStartPretty: time.Duration(traceTimings.LoadResponseStart).String(),
		DurationNano:             traceTimings.LoadResponseEnd - traceTimings.LoadResponseStart,
		DurationPretty:           time.Duration(traceTimings.LoadResponseEnd - traceTimings.LoadResponseStart).String(),
	})

}
