package sdk

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// ConfigField defines a configuration field for a processor.
type ConfigField struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
	Required    bool   `json:"required"`
	Default     any    `json:"default,omitempty"`
	EnvVar      string `json:"env_var,omitempty"`
}

// Definition describes a processor and its configuration/input/output schemas.
type Definition struct {
	ID           string        `json:"id"`
	Name         string        `json:"name"`
	Description  string        `json:"description"`
	Version      string        `json:"version"`
	Subjects     []string      `json:"subjects"`
	ConfigSchema []ConfigField `json:"config_schema,omitempty"`
	InputSchema  []SchemaField `json:"input_schema,omitempty"`
	OutputSchema []SchemaField `json:"output_schema,omitempty"`
	Factory      Factory       `json:"-"`
}

// Factory is a function that creates a Handler from a configuration map.
type Factory func(config map[string]any) (Handler, error)

// Registry stores processor definitions.
type Registry struct {
	defs map[string]*Definition
}

// NewRegistry creates a new processor Registry.
func NewRegistry() *Registry {
	return &Registry{defs: make(map[string]*Definition)}
}

// Register adds a processor definition to the registry.
func (r *Registry) Register(def *Definition) error {
	if def.ID == "" {
		return fmt.Errorf("id required")
	}
	if def.Factory == nil {
		return fmt.Errorf("factory required")
	}
	if len(def.Subjects) == 0 {
		return fmt.Errorf("subjects required")
	}
	r.defs[def.ID] = def
	return nil
}

// Get retrieves a processor definition by ID.
func (r *Registry) Get(id string) (*Definition, bool) {
	d, ok := r.defs[id]
	return d, ok
}

// List returns all registered processor definitions.
func (r *Registry) List() []*Definition {
	out := make([]*Definition, 0, len(r.defs))
	for _, d := range r.defs {
		out = append(out, d)
	}
	return out
}

// Create instantiates a processor Handler using the registered factory.
func (r *Registry) Create(id string, config map[string]any) (Handler, error) {
	def, ok := r.defs[id]
	if !ok {
		return nil, fmt.Errorf("processor not found: %s", id)
	}

	cfg, err := r.applyDefaults(def, config)
	if err != nil {
		return nil, err
	}

	return def.Factory(cfg)
}

func (r *Registry) applyDefaults(def *Definition, config map[string]any) (map[string]any, error) {
	result := make(map[string]any)

	for _, f := range def.ConfigSchema {
		v, exists := config[f.Name]
		if !exists {
			if f.Required && f.Default == nil {
				return nil, fmt.Errorf("missing required: %s", f.Name)
			}
			if f.Default != nil {
				result[f.Name] = f.Default
			}
			continue
		}
		result[f.Name] = v
	}

	for k, v := range config {
		if _, exists := result[k]; !exists {
			result[k] = v
		}
	}

	return result, nil
}

// JSON returns the JSON representation of the definition.
func (def *Definition) JSON() ([]byte, error) {
	return json.Marshal(def)
}

// Builder helps in creating processor definitions.
type Builder struct {
	def *Definition
}

// DefineProcessor starts a new processor definition with the given ID.
func DefineProcessor(id string) *Builder {
	return &Builder{def: &Definition{ID: id}}
}

// Name sets the name of the processor.
func (b *Builder) Name(n string) *Builder { b.def.Name = n; return b }

// Description sets the description of the processor.
func (b *Builder) Description(d string) *Builder { b.def.Description = d; return b }

// Version sets the version of the processor.
func (b *Builder) Version(v string) *Builder { b.def.Version = v; return b }

// Subjects sets the NATS subjects this processor handles.
func (b *Builder) Subjects(s ...string) *Builder { b.def.Subjects = s; return b }

// Config adds a configuration field to the processor definition.
func (b *Builder) Config(name, typ, desc string, required bool, def any) *Builder {
	b.def.ConfigSchema = append(b.def.ConfigSchema, ConfigField{
		Name: name, Type: typ, Description: desc, Required: required, Default: def,
	})
	return b
}

// ConfigEnv adds a configuration field that can be populated from an environment variable.
func (b *Builder) ConfigEnv(name, typ, desc, env string, def any) *Builder {
	b.def.ConfigSchema = append(b.def.ConfigSchema, ConfigField{
		Name: name, Type: typ, Description: desc, EnvVar: env, Default: def,
	})
	return b
}

// Input adds an input field to the processor definition schema.
func (b *Builder) Input(name, typ, desc string, required bool) *Builder {
	b.def.InputSchema = append(b.def.InputSchema, SchemaField{
		Name: name, Type: typ, Description: desc, Required: required,
	})
	return b
}

// Output adds an output field to the processor definition schema.
func (b *Builder) Output(name, typ, desc string) *Builder {
	b.def.OutputSchema = append(b.def.OutputSchema, SchemaField{
		Name: name, Type: typ, Description: desc,
	})
	return b
}

// Factory sets the factory function for the processor.
func (b *Builder) Factory(f Factory) *Builder {
	b.def.Factory = f
	return b
}

// Build returns the final processor definition.
func (b *Builder) Build() *Definition { return b.def }

// MustRegister builds and registers the processor, panicking on failure.
func (b *Builder) MustRegister(r *Registry) *Definition {
	if err := r.Register(b.def); err != nil {
		panic(err)
	}
	return b.def
}

// Configurable is a helper for processors that need to access their configuration.
type Configurable struct {
	cfg map[string]any
}

// SetConfig sets the configuration for the processor.
func (c *Configurable) SetConfig(cfg map[string]any) { c.cfg = cfg }

// Cfg returns the current configuration map.
func (c *Configurable) Cfg() map[string]any { return c.cfg }

// String retrieves a string configuration value.
func (c *Configurable) String(k string) string {
	if v, ok := c.cfg[k].(string); ok {
		return v
	}
	return ""
}

// Int retrieves an integer configuration value.
func (c *Configurable) Int(k string) int {
	switch v := c.cfg[k].(type) {
	case int:
		return v
	case float64:
		return int(v)
	}
	return 0
}

// Bool retrieves a boolean configuration value.
func (c *Configurable) Bool(k string) bool {
	if v, ok := c.cfg[k].(bool); ok {
		return v
	}
	return false
}

// ParseConfig decodes a configuration map into a struct.
func ParseConfig[T any](cfg map[string]any) (*T, error) {
	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ConfigSchemaFrom generates a list of configuration fields from a struct using reflection.
func ConfigSchemaFrom(v any) []ConfigField {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	var fields []ConfigField
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := f.Tag.Get("json")
		if name == "" || name == "-" {
			continue
		}

		typ := "string"
		switch f.Type.Kind() {
		case reflect.Int, reflect.Int64:
			typ = "int"
		case reflect.Float64:
			typ = "float"
		case reflect.Bool:
			typ = "bool"
		case reflect.Slice:
			typ = "array"
		case reflect.Map, reflect.Struct:
			typ = "object"
		}

		_, hasDefault := f.Tag.Lookup("default")
		fields = append(fields, ConfigField{
			Name:        name,
			Type:        typ,
			Description: f.Tag.Get("desc"),
			Required:    !hasDefault,
		})
	}
	return fields
}
