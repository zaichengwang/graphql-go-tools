package plan

import (
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"
)

type Kind int

const (
	SynchronousResponseKind Kind = iota + 1
	SubscriptionResponseKind
)

type Plan interface {
	PlanKind() Kind
	SetFlushInterval(interval int64)
	NodeSuggestions() []NodeSuggestion
	SetNodeSuggestions(suggestions []NodeSuggestion)
}

type SynchronousResponsePlan struct {
	Response        *resolve.GraphQLResponse
	FlushInterval   int64
	nodeSuggestions []NodeSuggestion
}

func (s *SynchronousResponsePlan) SetFlushInterval(interval int64) {
	s.FlushInterval = interval
}

func (_ *SynchronousResponsePlan) PlanKind() Kind {
	return SynchronousResponseKind
}

func (s *SynchronousResponsePlan) SetNodeSuggestions(suggestions []NodeSuggestion) {
	s.nodeSuggestions = suggestions
}
func (s *SynchronousResponsePlan) NodeSuggestions() []NodeSuggestion {
	return s.nodeSuggestions
}

type SubscriptionResponsePlan struct {
	Response        *resolve.GraphQLSubscription
	FlushInterval   int64
	nodeSuggestions []NodeSuggestion
}

func (s *SubscriptionResponsePlan) SetFlushInterval(interval int64) {
	s.FlushInterval = interval
}

func (_ *SubscriptionResponsePlan) PlanKind() Kind {
	return SubscriptionResponseKind
}

func (s *SubscriptionResponsePlan) SetNodeSuggestions(suggestions []NodeSuggestion) {
	s.nodeSuggestions = suggestions
}

func (s *SubscriptionResponsePlan) NodeSuggestions() []NodeSuggestion {
	return s.nodeSuggestions
}
