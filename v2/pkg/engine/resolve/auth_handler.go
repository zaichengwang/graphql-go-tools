package resolve

import (
	"context"
	"encoding/json"
	"io"
)

// AuthHandler handles field-level authorization
type AuthHandler struct {
	ctx context.Context
}

// NewAuthHandler creates a new AuthHandler instance
func NewAuthHandler(ctx context.Context) *AuthHandler {
	return &AuthHandler{
		ctx: ctx,
	}
}

// AuthorizeField is the core authorization logic used by both pre and post fetch
func (h *AuthHandler) AuthorizeField(coordinate GraphCoordinate) (bool, string) {
	if !coordinate.HasAuthorizationRule || len(coordinate.AuthDirectives) == 0 {
		return true, ""
	}

	// Deserialize auth directives from JSON
	var directives map[string]DirectiveInfo
	if err := json.Unmarshal(coordinate.AuthDirectives, &directives); err != nil {
		// If we can't parse directives, fail closed (deny access)
		return false, "failed to parse auth directives"
	}

	// Process each directive
	for name, directive := range directives {
		switch name {
		case "nullifyUnauthorizedEdges":
			if condition, ok := directive.Arguments["condition"].(bool); ok && !condition {
				return false, "edge access denied by nullifyUnauthorizedEdges directive"
			}
		}
	}

	return true, ""
}

// AuthorizePreFetch is a noop since we only do post-fetch authorization
func (h *AuthHandler) AuthorizePreFetch(ctx *Context, dataSourceID string, input json.RawMessage, coordinate GraphCoordinate) (result *AuthorizationDeny, err error) {
	return nil, nil
}

// AuthorizeObjectField handles post-fetch authorization
func (h *AuthHandler) AuthorizeObjectField(ctx *Context, dataSourceID string, object json.RawMessage, coordinate GraphCoordinate) (result *AuthorizationDeny, err error) {
	if !coordinate.HasAuthorizationRule && len(coordinate.AuthDirectives) == 0 {
		return nil, nil
	}

	// Deserialize auth directives from JSON
	var directives map[string]DirectiveInfo
	if err := json.Unmarshal(coordinate.AuthDirectives, &directives); err != nil {
		return &AuthorizationDeny{Reason: "failed to parse auth directives"}, nil
	}

	// Process each directive
	for name, directive := range directives {
		switch name {
		case "nullifyUnauthorizedEdges":
			if condition, ok := directive.Arguments["condition"].(bool); ok && !condition {
				return &AuthorizationDeny{Reason: "edge access denied by nullifyUnauthorizedEdges directive"}, nil
			}
		}
	}

	return nil, nil
}

func (h *AuthHandler) HasResponseExtensionData(ctx *Context) bool {
	return false
}

func (h *AuthHandler) RenderResponseExtension(ctx *Context, out io.Writer) error {
	return nil
}
