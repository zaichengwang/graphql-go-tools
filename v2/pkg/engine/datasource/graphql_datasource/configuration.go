package graphql_datasource

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"github.com/wundergraph/graphql-go-tools/v2/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/astparser"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/asttransform"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/plan"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/federation"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/operationreport"
)

type ConfigurationInput struct {
	Fetch                  *FetchConfiguration
	Subscription           *SubscriptionConfiguration
	SchemaConfiguration    *SchemaConfiguration
	CustomScalarTypeFields []SingleTypeField
}

type Configuration struct {
	fetch                  *FetchConfiguration
	subscription           *SubscriptionConfiguration
	schemaConfiguration    SchemaConfiguration
	customScalarTypeFields []SingleTypeField
}

func (c *Configuration) FieldDirectives() map[string]map[string][]plan.DirectiveConfiguration {
	return c.schemaConfiguration.fieldDirectives
}

func NewConfiguration(input ConfigurationInput) (Configuration, error) {
	cfg := Configuration{
		customScalarTypeFields: input.CustomScalarTypeFields,
	}

	if input.SchemaConfiguration == nil {
		return Configuration{}, errors.New("schema configuration is required")
	}
	if input.SchemaConfiguration.upstreamSchema == "" {
		return Configuration{}, errors.New("schema configuration is invalid: upstream schema is required")
	}

	cfg.schemaConfiguration = *input.SchemaConfiguration

	if input.Fetch == nil && input.Subscription == nil {
		return Configuration{}, errors.New("fetch or subscription configuration is required")
	}

	if input.Fetch != nil {
		cfg.fetch = input.Fetch

		if cfg.fetch.Method == "" {
			cfg.fetch.Method = "POST"
		}
	}

	if input.Subscription != nil {
		cfg.subscription = input.Subscription

		if cfg.fetch != nil {
			if len(cfg.subscription.Header) == 0 && len(cfg.fetch.Header) > 0 {
				cfg.subscription.Header = cfg.fetch.Header
			}

			if cfg.subscription.URL == "" {
				cfg.subscription.URL = cfg.fetch.URL
			}
		}
	}

	return cfg, nil
}

func (c *Configuration) UpstreamSchema() (*ast.Document, error) {
	if c.schemaConfiguration.upstreamSchemaAst == nil {
		return nil, errors.New("upstream schema is not parsed")
	}

	return c.schemaConfiguration.upstreamSchemaAst, nil
}

func (c *Configuration) IsFederationEnabled() bool {
	return c.schemaConfiguration.federation != nil && c.schemaConfiguration.federation.Enabled
}

func (c *Configuration) FederationConfiguration() *FederationConfiguration {
	return c.schemaConfiguration.federation
}

type SingleTypeField struct {
	TypeName  string
	FieldName string
}

type SubscriptionConfiguration struct {
	URL           string
	Header        http.Header
	UseSSE        bool
	SSEMethodPost bool
	// ForwardedClientHeaderNames indicates headers names that might be forwarded from the
	// client to the upstream server. This is used to determine which connections
	// can be multiplexed together, but the subscription engine does not forward
	// these headers by itself.
	ForwardedClientHeaderNames []string
	// ForwardedClientHeaderRegularExpressions regular expressions that if matched to the header
	// name might be forwarded from the client to the upstream server. This is used to determine
	// which connections can be multiplexed together, but the subscription engine does not forward
	// these headers by itself.
	ForwardedClientHeaderRegularExpressions []*regexp.Regexp
}

type FetchConfiguration struct {
	URL    string
	Method string
	Header http.Header
}

type FederationConfiguration struct {
	Enabled    bool
	ServiceSDL string
}

type SchemaConfiguration struct {
	upstreamSchema    string
	upstreamSchemaAst *ast.Document
	federation        *FederationConfiguration
	// Change from []string to []DirectiveConfiguration
	fieldDirectives map[string]map[string][]plan.DirectiveConfiguration
}

func (c *SchemaConfiguration) FederationServiceSDL() string {
	if c.federation == nil {
		return ""
	}

	return c.federation.ServiceSDL
}

func (c *SchemaConfiguration) IsFederationEnabled() bool {
	return c.federation != nil && c.federation.Enabled
}

func NewSchemaConfiguration(upstreamSchema string, federationCfg *FederationConfiguration) (*SchemaConfiguration, error) {
	cfg := &SchemaConfiguration{
		upstreamSchema: upstreamSchema,
		federation:     federationCfg,
		// Initialize with the new type
		fieldDirectives: make(map[string]map[string][]plan.DirectiveConfiguration),
	}

	if cfg.upstreamSchema == "" {
		return nil, fmt.Errorf("upstream schema is required")
	}

	definition := ast.NewSmallDocument()
	definitionParser := astparser.NewParser()
	report := &operationreport.Report{}

	if cfg.federation != nil && cfg.federation.Enabled {
		if cfg.federation.ServiceSDL == "" {
			return nil, fmt.Errorf("federation service SDL is required")
		}

		federationSchema, err := federation.BuildFederationSchema(cfg.upstreamSchema, cfg.federation.ServiceSDL)
		if err != nil {
			return nil, fmt.Errorf("unable to build federation schema: %v", err)
		}
		definition.Input.ResetInputString(federationSchema)
		definitionParser.Parse(definition, report)
		if report.HasErrors() {
			return nil, fmt.Errorf("unable to parse federation schema: %v", report)
		}
	} else {
		definition.Input.ResetInputString(cfg.upstreamSchema)
		definitionParser.Parse(definition, report)
		if report.HasErrors() {
			return nil, fmt.Errorf("unable to parse upstream schema: %v", report)
		}

		if err := asttransform.MergeDefinitionWithBaseSchema(definition); err != nil {
			return nil, fmt.Errorf("unable to merge upstream schema with base schema: %v", err)
		}
	}

	// Update directive indexing logic
	for i := range definition.ObjectTypeDefinitions {
		objectType := definition.ObjectTypeDefinitions[i]
		if !objectType.HasFieldDefinitions {
			continue
		}

		typeName := definition.ObjectTypeDefinitionNameString(i)
		cfg.fieldDirectives[typeName] = make(map[string][]plan.DirectiveConfiguration)

		for _, fieldDefRef := range objectType.FieldsDefinition.Refs {
			fieldDef := definition.FieldDefinitions[fieldDefRef]
			fieldName := definition.FieldDefinitionNameString(fieldDefRef)

			if fieldDef.HasDirectives {
				directives := make([]plan.DirectiveConfiguration, 0, len(fieldDef.Directives.Refs))
				for _, directiveRef := range fieldDef.Directives.Refs {
					directiveName := definition.DirectiveNameString(directiveRef)

					// Create new DirectiveConfiguration
					directive := plan.DirectiveConfiguration{
						DirectiveName: directiveName,
						Arguments:     make(map[string]interface{}),
					}

					// Optional: Parse directive arguments if they exist
					if definition.Directives[directiveRef].HasArguments {
						for _, argRef := range definition.Directives[directiveRef].Arguments.Refs {
							argName := definition.ArgumentNameString(argRef)
							directive.Arguments[argName] = definition.ArgumentValue(argRef)
						}
					}

					directives = append(directives, directive)
				}
				cfg.fieldDirectives[typeName][fieldName] = directives
			}
		}
	}

	cfg.upstreamSchemaAst = definition

	return cfg, nil
}

// Update the HasFieldDirective method to work with DirectiveConfiguration
func (c *SchemaConfiguration) HasFieldDirective(typeName, fieldName, directiveName string) bool {
	if fieldDirectives, ok := c.fieldDirectives[typeName]; ok {
		if directives, ok := fieldDirectives[fieldName]; ok {
			for _, d := range directives {
				if d.DirectiveName == directiveName {
					return true
				}
			}
		}
	}
	return false
}

// Add a new helper method to get the full directive configuration
func (c *SchemaConfiguration) GetFieldDirectives(typeName, fieldName string) []plan.DirectiveConfiguration {
	if fieldDirectives, ok := c.fieldDirectives[typeName]; ok {
		if directives, ok := fieldDirectives[fieldName]; ok {
			return directives
		}
	}
	return nil
}
