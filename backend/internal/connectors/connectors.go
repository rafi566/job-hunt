package connectors

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ConnectorType represents source or destination categories.
type ConnectorType string

const (
	SourceType      ConnectorType = "source"
	DestinationType ConnectorType = "destination"
)

// Connector describes shared metadata returned to the UI.
type Connector struct {
	Name        string        `json:"name"`
	Type        ConnectorType `json:"type"`
	Description string        `json:"description"`
	SupportsDDL bool          `json:"supportsDDL"`
	MaxParallel int           `json:"maxParallel"`
}

// Source defines extraction behavior.
type Source interface {
	Info() Connector
	Validate(config map[string]string) error
	Extract(ctx context.Context, config map[string]string) (<-chan map[string]any, error)
}

// Destination defines load behavior.
type Destination interface {
	Info() Connector
	Validate(config map[string]string) error
	Load(ctx context.Context, config map[string]string, records <-chan map[string]any) error
}

// Registry maintains in-memory connector listings used by the API and UI.
type Registry struct {
	sources      map[string]Source
	destinations map[string]Destination
}

// NewRegistry builds the registry with the available connectors.
func NewRegistry() *Registry {
	r := &Registry{
		sources:      map[string]Source{},
		destinations: map[string]Destination{},
	}

	dbFields := []string{"host", "port", "user", "password", "database"}

	r.registerSources(
		newSimpleSource("mysql", "High-speed MySQL binlog reader", true, 8, dbFields, 50),
		newSimpleSource("postgres", "Logical replication with parallel snapshot", true, 8, dbFields, 50),
		newSimpleSource("sqlserver", "SQL Server CDC with snapshot fallback", true, 4, dbFields, 50),
		newSimpleSource("iceberg", "Snapshot reads over Apache Iceberg metadata", false, 6, []string{"catalog", "table", "warehouse"}, 30),
	)

	r.registerDestinations(
		newSimpleDestination("mysql", "Batch inserts with parallel writers", true, 8, dbFields),
		newSimpleDestination("postgres", "COPY protocol with conflict handling", true, 8, dbFields),
		newSimpleDestination("sqlserver", "Bulk copy optimized for columnstore", true, 4, dbFields),
	)

	return r
}

func (r *Registry) registerSources(items ...Source) {
	for _, item := range items {
		r.sources[item.Info().Name] = item
	}
}

func (r *Registry) registerDestinations(items ...Destination) {
	for _, item := range items {
		r.destinations[item.Info().Name] = item
	}
}

// Available returns all connectors as combined metadata.
func (r *Registry) Available() []Connector {
	var result []Connector
	for _, s := range r.sources {
		result = append(result, s.Info())
	}
	for _, d := range r.destinations {
		result = append(result, d.Info())
	}
	return result
}

// SourceByName fetches a registered source.
func (r *Registry) SourceByName(name string) (Source, error) {
	s, ok := r.sources[name]
	if !ok {
		return nil, fmt.Errorf("unknown source connector %s", name)
	}
	return s, nil
}

// DestinationByName fetches a registered destination.
func (r *Registry) DestinationByName(name string) (Destination, error) {
	d, ok := r.destinations[name]
	if !ok {
		return nil, fmt.Errorf("unknown destination connector %s", name)
	}
	return d, nil
}

// ValidateConnectorPair ensures source and destination are compatible.
func ValidateConnectorPair(src Connector, dst Connector) error {
	if src.Type != SourceType || dst.Type != DestinationType {
		return errors.New("invalid connector pairing")
	}
	if src.Name == dst.Name && src.Name == "iceberg" {
		return errors.New("iceberg cannot be a destination")
	}
	return nil
}

// simpleSource implements Source with predictable behavior so beginners can follow the flow.
type simpleSource struct {
	meta          Connector
	required      []string
	sampleRecords int
}

func newSimpleSource(name, description string, supportsDDL bool, maxParallel int, required []string, sampleRecords int) *simpleSource {
	return &simpleSource{
		meta: Connector{
			Name:        name,
			Type:        SourceType,
			Description: description,
			SupportsDDL: supportsDDL,
			MaxParallel: maxParallel,
		},
		required:      required,
		sampleRecords: sampleRecords,
	}
}

func (s *simpleSource) Info() Connector { return s.meta }

func (s *simpleSource) Validate(config map[string]string) error {
	return ensureRequiredFields(s.required, config)
}

func (s *simpleSource) Extract(ctx context.Context, config map[string]string) (<-chan map[string]any, error) {
	if err := s.Validate(config); err != nil {
		return nil, err
	}
	return produceFakeRecords(ctx, s.sampleRecords), nil
}

// simpleDestination mirrors simpleSource for load connectors.
type simpleDestination struct {
	meta     Connector
	required []string
}

func newSimpleDestination(name, description string, supportsDDL bool, maxParallel int, required []string) *simpleDestination {
	return &simpleDestination{
		meta: Connector{
			Name:        name,
			Type:        DestinationType,
			Description: description,
			SupportsDDL: supportsDDL,
			MaxParallel: maxParallel,
		},
		required: required,
	}
}

func (d *simpleDestination) Info() Connector { return d.meta }

func (d *simpleDestination) Validate(config map[string]string) error {
	return ensureRequiredFields(d.required, config)
}

func (d *simpleDestination) Load(ctx context.Context, config map[string]string, records <-chan map[string]any) error {
	if err := d.Validate(config); err != nil {
		return err
	}
	return drainRecords(ctx, records)
}

func ensureRequiredFields(required []string, config map[string]string) error {
	for _, key := range required {
		if config[key] == "" {
			return fmt.Errorf("missing required config %s", key)
		}
	}
	return nil
}

func produceFakeRecords(ctx context.Context, records int) <-chan map[string]any {
	out := make(chan map[string]any)
	go func() {
		defer close(out)
		for i := 0; i < records; i++ {
			select {
			case <-ctx.Done():
				return
			case out <- map[string]any{"id": i + 1, "payload": fmt.Sprintf("record-%d", i+1)}:
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()
	return out
}

func drainRecords(ctx context.Context, records <-chan map[string]any) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-records:
			if !ok {
				return nil
			}
		}
	}
}
