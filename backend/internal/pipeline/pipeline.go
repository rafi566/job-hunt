package pipeline

import (
	"context"
	"errors"
	"sort"
	"strings"
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
	if err := validatePipelineName(cfg.Name); err != nil {
		return err
	}
	src, dst, err := s.connectorsFor(cfg)
	if err != nil {
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

	result := make([]Config, 0, len(s.store))
	for _, cfg := range s.store {
		result = append(result, cfg)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// Run triggers extraction and load for a pipeline.
func (s *Service) Run(ctx context.Context, name string) Result {
	result := newResult(name)
	cfg, ok := s.getConfig(name)
	if !ok {
		result.finish(0, errors.New("pipeline not found"))
		return result
	}
	src, dst, err := s.connectorsFor(cfg)
	if err != nil {
		result.finish(0, err)
		return result
	}
	records, err := src.Extract(ctx, cfg.SourceConfig)
	if err != nil {
		result.finish(0, err)
		return result
	}
	counter := 0
	loadErr := dst.Load(ctx, cfg.DestConfig, Tee(records, func(map[string]any) {
		counter++
	}))
	result.finish(counter, loadErr)
	return result
}

func (s *Service) getConfig(name string) (Config, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfg, ok := s.store[name]
	return cfg, ok
}

func (s *Service) connectorsFor(cfg Config) (connectors.Source, connectors.Destination, error) {
	src, err := s.registry.SourceByName(cfg.SourceType)
	if err != nil {
		return nil, nil, err
	}
	dst, err := s.registry.DestinationByName(cfg.DestType)
	if err != nil {
		return nil, nil, err
	}
	if err := connectors.ValidateConnectorPair(src.Info(), dst.Info()); err != nil {
		return nil, nil, err
	}
	return src, dst, nil
}

func newResult(name string) Result {
	return Result{
		PipelineName: name,
		StartedAt:    time.Now(),
	}
}

func (r *Result) finish(records int, runErr error) {
	r.Records = records
	if runErr != nil {
		r.Error = runErr.Error()
	} else {
		r.Error = ""
	}
	r.FinishedAt = time.Now()
}

func validatePipelineName(name string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("pipeline name is required")
	}
	return nil
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
