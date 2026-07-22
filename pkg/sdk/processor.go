package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// Processor is the core interface for task execution.
// All processors are typed - external/dynamic processors use Dynamic as their type.
type Processor[In, Out any] interface {
	Process(ctx context.Context, input In) (Out, error)
}

// Func adapts a function to the Processor interface.
type Func[In, Out any] func(ctx context.Context, input In) (Out, error)

// Process implements the Processor interface for Func.
func (f Func[In, Out]) Process(ctx context.Context, input In) (Out, error) {
	return f(ctx, input)
}

// Dynamic is the type for unstructured data (external processors, dynamic schemas).
type Dynamic = map[string]any

// Handler is the internal interface used by the runner.
// All Processor[In, Out] implementations are wrapped to satisfy this.
type Handler interface {
	Handle(ctx context.Context, input Dynamic) (Dynamic, error)
}

// Wrap converts a typed Processor to a Handler for the runner.
func Wrap[In, Out any](p Processor[In, Out]) Handler {
	return &wrapper[In, Out]{proc: p}
}

// WrapFunc wraps a typed function.
func WrapFunc[In, Out any](f Func[In, Out]) Handler {
	return Wrap(f)
}

type wrapper[In, Out any] struct {
	proc      Processor[In, Out]
	validator *validator
}

func (w *wrapper[In, Out]) Handle(ctx context.Context, raw Dynamic) (Dynamic, error) {
	var in In

	// Check if In is Dynamic (map[string]any) - skip marshaling
	if _, isDynamic := any(&in).(*Dynamic); isDynamic {
		inPtr := any(&in).(*Dynamic)
		*inPtr = raw
	} else {
		// Unmarshal to typed struct
		data, err := json.Marshal(raw)
		if err != nil {
			return nil, fmt.Errorf("marshal input: %w", err)
		}
		if err := json.Unmarshal(data, &in); err != nil {
			return nil, fmt.Errorf("invalid input: %w", err)
		}
	}

	// Validate if schema is set
	if w.validator != nil {
		if errs := w.validator.validateInput(raw); len(errs) > 0 {
			return nil, &ValidationError{Field: "input", Errors: errs}
		}
	}

	out, err := w.proc.Process(ctx, in)
	if err != nil {
		return nil, err
	}

	// Check if Out is Dynamic - skip marshaling
	var result Dynamic
	if outDyn, isDynamic := any(out).(Dynamic); isDynamic {
		result = outDyn
	} else {
		outData, err := json.Marshal(out)
		if err != nil {
			return nil, fmt.Errorf("marshal output: %w", err)
		}
		if err := json.Unmarshal(outData, &result); err != nil {
			return nil, fmt.Errorf("unmarshal output: %w", err)
		}
	}

	if w.validator != nil {
		if errs := w.validator.validateOutput(result); len(errs) > 0 {
			return nil, &ValidationError{Field: "output", Errors: errs}
		}
	}

	return result, nil
}

// WithValidation adds schema validation to a processor.
func WithValidation[In, Out any](p Processor[In, Out], inputSchema, outputSchema []SchemaField) Handler {
	return &wrapper[In, Out]{
		proc:      p,
		validator: &validator{input: inputSchema, output: outputSchema},
	}
}

// TaskContext provides workflow metadata to processors that need it.
type TaskContext struct {
	WorkflowID string
	TaskID     string
	Subject    string
}

// ContextAware processors receive full task metadata.
type ContextAware[In, Out any] interface {
	Processor[In, Out]
	ProcessWithContext(ctx context.Context, taskCtx TaskContext, input In) (Out, error)
}

// ProgressReporter allows processors to report execution progress.
type ProgressReporter interface {
	ReportProgress(ctx context.Context, workflowID string, progress int) error
}

// Validation

// SchemaField defines a single field in a processor input/output schema.
type SchemaField struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
}

// ValidationError is returned when schema validation fails.
type ValidationError struct {
	Field  string
	Errors []string
}

// Error returns a string representation of the validation error.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s validation failed: %s", e.Field, strings.Join(e.Errors, "; "))
}

type validator struct {
	input  []SchemaField
	output []SchemaField
}

func (v *validator) validateInput(data Dynamic) []string {
	return validateSchema(data, v.input)
}

func (v *validator) validateOutput(data Dynamic) []string {
	return validateSchema(data, v.output)
}

func validateSchema(data Dynamic, schema []SchemaField) []string {
	var errs []string
	for _, f := range schema {
		val, exists := data[f.Name]
		if !exists {
			if f.Required {
				errs = append(errs, fmt.Sprintf("%s: required", f.Name))
			}
			continue
		}
		if err := validateType(f.Name, val, f.Type); err != "" {
			errs = append(errs, err)
		}
	}
	return errs
}

func validateType(name string, val any, typ string) string {
	if val == nil {
		return ""
	}
	rv := reflect.ValueOf(val)
	switch typ {
	case "string":
		if rv.Kind() != reflect.String {
			return fmt.Sprintf("%s: expected string", name)
		}
	case "int", "integer":
		switch rv.Kind() {
		case reflect.Float64:
			if f := rv.Float(); f != float64(int64(f)) {
				return fmt.Sprintf("%s: expected int", name)
			}
		case reflect.Int, reflect.Int64:
		default:
			return fmt.Sprintf("%s: expected int", name)
		}
	case "float", "number":
		switch rv.Kind() {
		case reflect.Float32, reflect.Float64, reflect.Int, reflect.Int64:
		default:
			return fmt.Sprintf("%s: expected number", name)
		}
	case "bool", "boolean":
		if rv.Kind() != reflect.Bool {
			return fmt.Sprintf("%s: expected bool", name)
		}
	case "array":
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			return fmt.Sprintf("%s: expected array", name)
		}
	case "object":
		if rv.Kind() != reflect.Map {
			return fmt.Sprintf("%s: expected object", name)
		}
	}
	return ""
}

// SchemaFrom generates schema from a struct type using reflection.
func SchemaFrom[T any]() []SchemaField {
	var zero T
	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	var fields []SchemaField
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		name := strings.Split(tag, ",")[0]
		if name == "" {
			continue
		}

		fields = append(fields, SchemaField{
			Name:        name,
			Type:        kindToType(f.Type.Kind()),
			Description: f.Tag.Get("desc"),
			Required:    f.Tag.Get("required") == "true",
		})
	}
	return fields
}

func kindToType(k reflect.Kind) string {
	switch k {
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "int"
	case reflect.Float32, reflect.Float64:
		return "float"
	case reflect.Bool:
		return "bool"
	case reflect.Slice, reflect.Array:
		return "array"
	case reflect.Map, reflect.Struct:
		return "object"
	default:
		return "any"
	}
}
