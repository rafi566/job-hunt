'use client';

import { useEffect, useMemo, useState } from 'react';

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || 'http://localhost:8080';

type ConnectorKind = 'source' | 'destination';

type ConfigMap = Record<string, string>;

interface Connector {
  name: string;
  type: ConnectorKind;
  description: string;
  supportsDDL: boolean;
  maxParallel: number;
}

interface PipelineConfig {
  name: string;
  sourceType: string;
  sourceConfig: ConfigMap;
  destType: string;
  destConfig: ConfigMap;
}

interface RunResult {
  pipelineName: string;
  startedAt: string;
  finishedAt: string;
  records: number;
  error?: string;
}

const DEFAULT_CONNECTORS: Connector[] = [
  {
    name: 'mysql',
    type: 'source',
    description: 'High-speed MySQL binlog reader',
    supportsDDL: true,
    maxParallel: 8,
  },
  {
    name: 'postgres',
    type: 'source',
    description: 'Logical replication with parallel snapshot',
    supportsDDL: true,
    maxParallel: 8,
  },
  {
    name: 'sqlserver',
    type: 'source',
    description: 'SQL Server CDC',
    supportsDDL: true,
    maxParallel: 4,
  },
  {
    name: 'iceberg',
    type: 'source',
    description: 'Apache Iceberg snapshot reader',
    supportsDDL: false,
    maxParallel: 6,
  },
  {
    name: 'mysql',
    type: 'destination',
    description: 'Parallel batch loader',
    supportsDDL: true,
    maxParallel: 8,
  },
  {
    name: 'postgres',
    type: 'destination',
    description: 'COPY optimized loader',
    supportsDDL: true,
    maxParallel: 8,
  },
  {
    name: 'sqlserver',
    type: 'destination',
    description: 'Columnstore-friendly loader',
    supportsDDL: true,
    maxParallel: 4,
  },
];

const DEFAULT_SOURCE_CONFIG: ConfigMap = {
  host: 'localhost',
  port: '5432',
  user: 'demo',
  password: 'demo',
  database: 'sample',
};

const DEFAULT_DEST_CONFIG: ConfigMap = {
  host: 'localhost',
  port: '5432',
  user: 'demo',
  password: 'demo',
  database: 'warehouse',
};

export default function HomePage() {
  const [connectors, setConnectors] = useState<Connector[]>(DEFAULT_CONNECTORS);
  const [pipelines, setPipelines] = useState<PipelineConfig[]>([]);
  const [name, setName] = useState('nightly-sync');
  const [selectedSrc, setSelectedSrc] = useState('mysql');
  const [selectedDst, setSelectedDst] = useState('postgres');
  const [running, setRunning] = useState(false);
  const [lastRun, setLastRun] = useState<RunResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const sourceOptions = useMemo(() => connectors.filter((c) => c.type === 'source'), [connectors]);
  const destOptions = useMemo(() => connectors.filter((c) => c.type === 'destination'), [connectors]);

  useEffect(() => {
    fetchConnectors();
    fetchPipelines();
  }, []);

  async function fetchConnectors() {
    try {
      const res = await fetch(`${API_BASE}/connectors`, { cache: 'no-cache' });
      if (!res.ok) throw new Error('failed');
      const json = (await res.json()) as Connector[];
      if (Array.isArray(json) && json.length > 0) {
        setConnectors(json);
      }
    } catch (err) {
      console.warn('Using fallback connectors', err);
    }
  }

  async function fetchPipelines() {
    try {
      const res = await fetch(`${API_BASE}/pipelines`, { cache: 'no-cache' });
      if (!res.ok) throw new Error('failed to load pipelines');
      const json = (await res.json()) as PipelineConfig[];
      if (Array.isArray(json)) {
        setPipelines(json);
      }
    } catch (err) {
      console.warn('Pipeline list fallback', err);
    }
  }

  async function createPipeline() {
    setError(null);
    const payload: PipelineConfig = {
      name: name.trim() || 'nightly-sync',
      sourceType: selectedSrc,
      destType: selectedDst,
      sourceConfig: { ...DEFAULT_SOURCE_CONFIG },
      destConfig: { ...DEFAULT_DEST_CONFIG },
    };

    try {
      const res = await fetch(`${API_BASE}/pipelines`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!res.ok) throw new Error(await res.text());
      setPipelines((prev) => [...prev.filter((p) => p.name !== payload.name), payload]);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    }
  }

  async function runPipeline(targetName: string) {
    setRunning(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/pipelines/${targetName}/run`, { method: 'POST' });
      if (!res.ok) throw new Error(await res.text());
      const data = (await res.json()) as RunResult;
      setLastRun(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setRunning(false);
    }
  }

  return (
    <div className="main-shell">
      <HeroSection />
      <ConnectorsSection connectors={connectors} />
      <section style={{ marginTop: 32, display: 'grid', gridTemplateColumns: '2fr 1.2fr', gap: 18 }}>
        <PipelineBuilder
          name={name}
          onNameChange={setName}
          selectedSrc={selectedSrc}
          selectedDst={selectedDst}
          onSelectSrc={setSelectedSrc}
          onSelectDst={setSelectedDst}
          onCreate={createPipeline}
          sourceOptions={sourceOptions}
          destOptions={destOptions}
          error={error}
        />
        <PipelineRuns pipelines={pipelines} onRun={runPipeline} running={running} lastRun={lastRun} />
      </section>
      <FooterNote />
    </div>
  );
}

function HeroSection() {
  return (
    <div className="hero">
      <div>
        <div className="badge">Reliable pipelines • Zero-code orchestration</div>
        <h1>DataFlow Studio</h1>
        <p>
          Build Airbyte-style extracts with a Fivetran-inspired experience. Move data from MySQL, SQL Server, Postgres,
          and Apache Iceberg into your favorite destinations with resilient, parallelized pipelines.
        </p>
      </div>
      <div className="card">
        <h3>Throughput snapshot</h3>
        <p className="pill">
          <strong>75k</strong> rows/min • live validation
        </p>
        <p className="pill" style={{ marginTop: 10 }}>
          <strong>Latency</strong> &lt; 150ms hops
        </p>
      </div>
    </div>
  );
}

interface ConnectorsSectionProps {
  connectors: Connector[];
}

function ConnectorsSection({ connectors }: ConnectorsSectionProps) {
  return (
    <section style={{ marginTop: 28 }}>
      <h2>Connectors</h2>
      <div className="grid">
        {connectors.map((connector) => (
          <ConnectorCard key={`${connector.type}-${connector.name}`} connector={connector} />
        ))}
      </div>
    </section>
  );
}

interface ConnectorCardProps {
  connector: Connector;
}

function ConnectorCard({ connector }: ConnectorCardProps) {
  return (
    <div className="card">
      <div className="pill" style={{ marginBottom: 10 }}>
        {connector.type.toUpperCase()} • parallel x{connector.maxParallel}
      </div>
      <h3>{connector.name}</h3>
      <p>{connector.description}</p>
      <p style={{ marginTop: 8, fontSize: 13 }}>
        {connector.supportsDDL ? 'DDL aware' : 'Snapshot only'} • {connector.maxParallel} workers
      </p>
    </div>
  );
}

interface PipelineBuilderProps {
  name: string;
  onNameChange: (value: string) => void;
  selectedSrc: string;
  selectedDst: string;
  onSelectSrc: (value: string) => void;
  onSelectDst: (value: string) => void;
  onCreate: () => void;
  sourceOptions: Connector[];
  destOptions: Connector[];
  error: string | null;
}

function PipelineBuilder({
  name,
  onNameChange,
  selectedSrc,
  selectedDst,
  onSelectSrc,
  onSelectDst,
  onCreate,
  sourceOptions,
  destOptions,
  error,
}: PipelineBuilderProps) {
  return (
    <div className="form-panel">
      <h2>Compose pipeline</h2>
      <label className="label">Pipeline name</label>
      <input value={name} onChange={(event) => onNameChange(event.target.value)} placeholder="nightly-sync" />

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
        <div>
          <label className="label">Source</label>
          <select value={selectedSrc} onChange={(event) => onSelectSrc(event.target.value)}>
            {sourceOptions.map((s) => (
              <option key={s.name} value={s.name}>
                {s.name}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label className="label">Destination</label>
          <select value={selectedDst} onChange={(event) => onSelectDst(event.target.value)}>
            {destOptions.map((d) => (
              <option key={d.name} value={d.name}>
                {d.name}
              </option>
            ))}
          </select>
        </div>
      </div>

      <button onClick={onCreate}>Create pipeline</button>
      {error && <p style={{ color: '#fca5a5', marginTop: 8 }}>{error}</p>}
    </div>
  );
}

interface PipelineRunsProps {
  pipelines: PipelineConfig[];
  onRun: (name: string) => void;
  running: boolean;
  lastRun: RunResult | null;
}

function PipelineRuns({ pipelines, onRun, running, lastRun }: PipelineRunsProps) {
  return (
    <div className="card">
      <h3>Runs</h3>
      {pipelines.length === 0 ? (
        <p>No pipelines yet — create one to start syncing.</p>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Route</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {pipelines.map((p) => (
              <tr key={p.name}>
                <td>{p.name}</td>
                <td>
                  {p.sourceType} ➜ {p.destType}
                </td>
                <td style={{ textAlign: 'right' }}>
                  <button disabled={running} onClick={() => onRun(p.name)}>
                    {running ? 'Running…' : 'Run now'}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {lastRun && (
        <div className="pill" style={{ marginTop: 10 }}>
          {lastRun.pipelineName} processed {lastRun.records} rows in {formatDuration(lastRun.startedAt, lastRun.finishedAt)}
        </div>
      )}
    </div>
  );
}

function FooterNote() {
  return (
    <p className="footer-note">
      Tip: set NEXT_PUBLIC_API_BASE to point at your Go API (default http://localhost:8080). The UI gracefully falls back to
      demo data when the API is unreachable.
    </p>
  );
}

function formatDuration(start: string, end: string) {
  const ms = new Date(end).getTime() - new Date(start).getTime();
  if (Number.isNaN(ms)) return 'a few moments';
  return `${ms} ms`;
}
