package pipeline

import (
	"context"
	"errors"
	"sync"
	"time"

	"job-hunt/backend/internal/connectors"
)

// Config defines pipeline pairing between source and destination.
type Config struct {
	Name         string            `json:"name"`
	SourceType   string            `json:"sourceType"`
	SourceConfig map[string]string `json:"sourceConfig"`
	DestType     string            `json:"destType"`
	DestConfig   map[string]string `json:"destConfig"`
}

// Result captures execution state.
type Result struct {
	PipelineName string    `json:"pipelineName"`
	StartedAt    time.Time `json:"startedAt"`
	FinishedAt   time.Time `json:"finishedAt"`
	Records      int       `json:"records"`
	Error        string    `json:"error,omitempty"`
}

// Service owns registry and execution control.
type Service struct {
	registry *connectors.Registry
	store    map[string]Config
	mu       sync.RWMutex
}

// NewService builds a service with in-memory storage.
func NewService(reg *connectors.Registry) *Service {
	return &Service{registry: reg, store: map[string]Config{}}
}

// Create stores a pipeline definition.
func (s *Service) Create(cfg Config) error {
	if cfg.Name == "" {
		return errors.New("pipeline name is required")
	}
	src, err := s.registry.SourceByName(cfg.SourceType)
	if err != nil {
		return err
	}
	dst, err := s.registry.DestinationByName(cfg.DestType)
	if err != nil {
		return err
	}
	if err := connectors.ValidateConnectorPair(src.Info(), dst.Info()); err != nil {
		return err
	}
	if err := src.Validate(cfg.SourceConfig); err != nil {
		return err
	}
	if err := dst.Validate(cfg.DestConfig); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[cfg.Name] = cfg
	return nil
}

// List returns all pipeline configs.
func (s *Service) List() []Config {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []Config
	for _, cfg := range s.store {
		result = append(result, cfg)
	}
	return result
}

// Run triggers extraction and load for a pipeline.
func (s *Service) Run(ctx context.Context, name string) Result {
	s.mu.RLock()
	cfg, ok := s.store[name]
	s.mu.RUnlock()

	res := Result{
		PipelineName: name,
		StartedAt:    time.Now(),
	}

	if !ok {
		res.Error = "pipeline not found"
		res.FinishedAt = time.Now()
		return res
	}

	src, err := s.registry.SourceByName(cfg.SourceType)
	if err != nil {
		res.Error = err.Error()
		res.FinishedAt = time.Now()
		return res
	}
	dst, err := s.registry.DestinationByName(cfg.DestType)
	if err != nil {
		res.Error = err.Error()
		res.FinishedAt = time.Now()
		return res
	}

	records, err := src.Extract(ctx, cfg.SourceConfig)
	if err != nil {
		res.Error = err.Error()
		res.FinishedAt = time.Now()
		return res
	}

	// fan-out to count processed rows while loading
	counter := 0
	loadErr := dst.Load(ctx, cfg.DestConfig, Tee(records, func(m map[string]any) {
		counter++
	}))

	if loadErr != nil {
		res.Error = loadErr.Error()
	}
	res.Records = counter
	res.FinishedAt = time.Now()
	return res
}

// Tee duplicates record consumption with a side effect function.
func Tee(in <-chan map[string]any, fn func(map[string]any)) <-chan map[string]any {
	out := make(chan map[string]any)
	go func() {
		defer close(out)
		for record := range in {
			fn(record)
			out <- record
		}
	}()
	return out
}
