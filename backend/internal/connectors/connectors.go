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

	for _, src := range []Source{
		&MySQLSource{},
		&PostgresSource{},
		&SQLServerSource{},
		&IcebergSource{},
	} {
		r.sources[src.Info().Name] = src
	}

	for _, dst := range []Destination{
		&MySQLDestination{},
		&PostgresDestination{},
		&SQLServerDestination{},
	} {
		r.destinations[dst.Info().Name] = dst
	}

	return r
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

// simulateValidation enforces the presence of fields without talking to external systems.
func simulateValidation(required []string, config map[string]string) error {
	for _, key := range required {
		if config[key] == "" {
			return fmt.Errorf("missing required config %s", key)
		}
	}
	return nil
}

// simulateTransfer mirrors network throughput with deterministic pacing.
func simulateTransfer(ctx context.Context, records int) <-chan map[string]any {
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

// consumeTransfer drains the channel to mimic load operations.
func consumeTransfer(ctx context.Context, records <-chan map[string]any) error {
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

// Basic connector implementations below operate in-memory while preserving validation paths.

// MySQLSource extracts from MySQL.
type MySQLSource struct{ meta Connector }

func (s *MySQLSource) ensureMeta() {
	if s.meta.Name != "" {
		return
	}
	s.meta = Connector{
		Name:        "mysql",
		Type:        SourceType,
		Description: "High-speed MySQL binlog reader",
		SupportsDDL: true,
		MaxParallel: 8,
	}
}

func (s *MySQLSource) Info() Connector {
	s.ensureMeta()
	return s.meta
}

func (s *MySQLSource) Validate(config map[string]string) error {
	s.ensureMeta()
	return simulateValidation([]string{"host", "port", "user", "password", "database"}, config)
}

func (s *MySQLSource) Extract(ctx context.Context, config map[string]string) (<-chan map[string]any, error) {
	if err := s.Validate(config); err != nil {
		return nil, err
	}
	return simulateTransfer(ctx, 50), nil
}

// PostgresSource extracts from Postgres logical replication.
type PostgresSource struct{ meta Connector }

func (s *PostgresSource) ensureMeta() {
	if s.meta.Name != "" {
		return
	}
	s.meta = Connector{
		Name:        "postgres",
		Type:        SourceType,
		Description: "Logical replication with parallel snapshot",
		SupportsDDL: true,
		MaxParallel: 8,
	}
}

func (s *PostgresSource) Info() Connector {
	s.ensureMeta()
	return s.meta
}

func (s *PostgresSource) Validate(config map[string]string) error {
	s.ensureMeta()
	return simulateValidation([]string{"host", "port", "user", "password", "database"}, config)
}

func (s *PostgresSource) Extract(ctx context.Context, config map[string]string) (<-chan map[string]any, error) {
	if err := s.Validate(config); err != nil {
		return nil, err
	}
	return simulateTransfer(ctx, 50), nil
}

// SQLServerSource extracts from SQL Server CDC.
type SQLServerSource struct{ meta Connector }

func (s *SQLServerSource) ensureMeta() {
	if s.meta.Name != "" {
		return
	}
	s.meta = Connector{
		Name:        "sqlserver",
		Type:        SourceType,
		Description: "SQL Server CDC with snapshot fallback",
		SupportsDDL: true,
		MaxParallel: 4,
	}
}

func (s *SQLServerSource) Info() Connector {
	s.ensureMeta()
	return s.meta
}

func (s *SQLServerSource) Validate(config map[string]string) error {
	s.ensureMeta()
	return simulateValidation([]string{"host", "port", "user", "password", "database"}, config)
}

func (s *SQLServerSource) Extract(ctx context.Context, config map[string]string) (<-chan map[string]any, error) {
	if err := s.Validate(config); err != nil {
		return nil, err
	}
	return simulateTransfer(ctx, 50), nil
}

// IcebergSource extracts from Apache Iceberg tables.
type IcebergSource struct{ meta Connector }

func (s *IcebergSource) ensureMeta() {
	if s.meta.Name != "" {
		return
	}
	s.meta = Connector{
		Name:        "iceberg",
		Type:        SourceType,
		Description: "Snapshot reads over Apache Iceberg metadata",
		SupportsDDL: false,
		MaxParallel: 6,
	}
}

func (s *IcebergSource) Info() Connector {
	s.ensureMeta()
	return s.meta
}

func (s *IcebergSource) Validate(config map[string]string) error {
	s.ensureMeta()
	return simulateValidation([]string{"catalog", "table", "warehouse"}, config)
}

func (s *IcebergSource) Extract(ctx context.Context, config map[string]string) (<-chan map[string]any, error) {
	if err := s.Validate(config); err != nil {
		return nil, err
	}
	return simulateTransfer(ctx, 30), nil
}

// MySQLDestination loads into MySQL.
type MySQLDestination struct{ meta Connector }

func (d *MySQLDestination) ensureMeta() {
	if d.meta.Name != "" {
		return
	}
	d.meta = Connector{
		Name:        "mysql",
		Type:        DestinationType,
		Description: "Batch inserts with parallel writers",
		SupportsDDL: true,
		MaxParallel: 8,
	}
}

func (d *MySQLDestination) Info() Connector {
	d.ensureMeta()
	return d.meta
}

func (d *MySQLDestination) Validate(config map[string]string) error {
	d.ensureMeta()
	return simulateValidation([]string{"host", "port", "user", "password", "database"}, config)
}

func (d *MySQLDestination) Load(ctx context.Context, config map[string]string, records <-chan map[string]any) error {
	if err := d.Validate(config); err != nil {
		return err
	}
	return consumeTransfer(ctx, records)
}

// PostgresDestination loads into Postgres.
type PostgresDestination struct{ meta Connector }

func (d *PostgresDestination) ensureMeta() {
	if d.meta.Name != "" {
		return
	}
	d.meta = Connector{
		Name:        "postgres",
		Type:        DestinationType,
		Description: "COPY protocol with conflict handling",
		SupportsDDL: true,
		MaxParallel: 8,
	}
}

func (d *PostgresDestination) Info() Connector {
	d.ensureMeta()
	return d.meta
}

func (d *PostgresDestination) Validate(config map[string]string) error {
	d.ensureMeta()
	return simulateValidation([]string{"host", "port", "user", "password", "database"}, config)
}

func (d *PostgresDestination) Load(ctx context.Context, config map[string]string, records <-chan map[string]any) error {
	if err := d.Validate(config); err != nil {
		return err
	}
	return consumeTransfer(ctx, records)
}

// SQLServerDestination loads into SQL Server.
type SQLServerDestination struct{ meta Connector }

func (d *SQLServerDestination) ensureMeta() {
	if d.meta.Name != "" {
		return
	}
	d.meta = Connector{
		Name:        "sqlserver",
		Type:        DestinationType,
		Description: "Bulk copy optimized for columnstore",
		SupportsDDL: true,
		MaxParallel: 4,
	}
}

func (d *SQLServerDestination) Info() Connector {
	d.ensureMeta()
	return d.meta
}

func (d *SQLServerDestination) Validate(config map[string]string) error {
	d.ensureMeta()
	return simulateValidation([]string{"host", "port", "user", "password", "database"}, config)
}

func (d *SQLServerDestination) Load(ctx context.Context, config map[string]string, records <-chan map[string]any) error {
	if err := d.Validate(config); err != nil {
		return err
	}
	return consumeTransfer(ctx, records)
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
