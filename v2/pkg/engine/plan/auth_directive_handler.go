package plan

import (
	"github.com/wundergraph/graphql-go-tools/v2/pkg/ast"
)

type AuthDirectiveHandler struct {
	operation  *ast.Document
	definition *ast.Document
}

func NewAuthDirectiveHandler(operation, definition *ast.Document) *AuthDirectiveHandler {
	return &AuthDirectiveHandler{
		operation:  operation,
		definition: definition,
	}
}

func (h *AuthDirectiveHandler) HandleAuthDirective(ref int, fieldConfig *FieldConfiguration) {
	directiveName := h.operation.DirectiveNameString(ref)

	switch directiveName {
	case "nullifyUnauthorizedEdges":
		if fieldConfig != nil {
			args := make(map[string]interface{})
			// Parse directive arguments if they exist
			if directiveArgs := h.operation.Directives[ref].Arguments.Refs; len(directiveArgs) > 0 {
				for _, argRef := range directiveArgs {
					argName := h.operation.ArgumentNameString(argRef)
					argValue := h.operation.ArgumentValue(argRef)
					args[argName] = h.parseArgumentValue(argValue)
				}
			}

			// Store directive info in field configuration
			if fieldConfig.Directives == nil {
				fieldConfig.Directives = make(map[string]DirectiveConfiguration)
			}
			fieldConfig.Directives[directiveName] = DirectiveConfiguration{
				DirectiveName: directiveName,
				Arguments:     args,
			}
			fieldConfig.HasAuthorizationRule = true
		}
	}
}

func (h *AuthDirectiveHandler) parseArgumentValue(value ast.Value) interface{} {
	switch value.Kind {
	case ast.ValueKindString:
		return h.operation.StringValueContentString(value.Ref)
	case ast.ValueKindInteger:
		return h.operation.IntValueAsInt(value.Ref)
	case ast.ValueKindBoolean:
		return h.operation.BooleanValue(value.Ref)
	default:
		return nil
	}
}
