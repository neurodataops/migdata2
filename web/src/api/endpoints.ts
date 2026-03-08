import client from './client'

// Auth
export const login = (username: string, password: string) =>
  client.post('/auth/login', { username, password }).then((r) => r.data)

export const register = (username: string, password: string) =>
  client.post('/auth/register', { username, password }).then((r) => r.data)

// Config
export const getConfig = () =>
  client.get('/config').then((r) => r.data)

export const updatePlatform = (data: { source_adapter?: string; target_platform?: string }) =>
  client.put('/config/platform', data).then((r) => r.data)

export const updateThreshold = (confidence_threshold: number) =>
  client.put('/config/threshold', { confidence_threshold }).then((r) => r.data)

// Connection
export const testSourceMock = () =>
  client.post('/connection/test-source/mock').then((r) => r.data)

export const testSourceRedshift = (data: object) =>
  client.post('/connection/test-source/redshift', data).then((r) => r.data)

export const testSourceSnowflake = (data: object) =>
  client.post('/connection/test-source/snowflake', data).then((r) => r.data)

export const testTargetMock = () =>
  client.post('/connection/test-target/mock').then((r) => r.data)

// Pipeline
export const runPipeline = (source_platform: string, use_mock: boolean, selected_schemas: string[] = []) =>
  client.post('/pipeline/run', { source_platform, use_mock, selected_schemas }).then((r) => r.data)

export const getPipelineStatus = () =>
  client.get('/pipeline/status').then((r) => r.data)

// Catalog
export const getCatalog = (schemas?: string) =>
  client.get('/catalog', { params: schemas ? { schemas } : {} }).then((r) => r.data)

export const getSchemas = () =>
  client.get('/catalog/schemas').then((r) => r.data)

export const getRelationships = (schemas?: string) =>
  client.get('/catalog/relationships', { params: schemas ? { schemas } : {} }).then((r) => r.data)

export const getLineage = (schemas?: string) =>
  client.get('/catalog/lineage', { params: schemas ? { schemas } : {} }).then((r) => r.data)

// Conversion
export const getConversion = () =>
  client.get('/conversion').then((r) => r.data)

export const getConversionObjects = (schemas?: string) =>
  client.get('/conversion/objects', { params: schemas ? { schemas } : {} }).then((r) => r.data)

export const getSqlComparison = (schemas?: string) =>
  client.get('/conversion/sql-comparison', { params: schemas ? { schemas } : {} }).then((r) => r.data)

// Validation
export const getValidation = () =>
  client.get('/validation').then((r) => r.data)

export const getConfidence = (schemas?: string) =>
  client.get('/validation/confidence', { params: schemas ? { schemas } : {} }).then((r) => r.data)

export const getLoadSummary = () =>
  client.get('/validation/load-summary').then((r) => r.data)

// Query logs
export const getQueryTimeline = () =>
  client.get('/query-logs/timeline').then((r) => r.data)

// Agents
export const getLlmProviders = () =>
  client.get('/agents/llm-providers').then((r) => r.data)

export const setLlmProvider = (provider_id: string) =>
  client.put('/agents/llm-provider', { provider_id }).then((r) => r.data)

export const listAgents = () =>
  client.get('/agents/').then((r) => r.data)

export const transpileDDL = (source_ddl: string, task_id: number = 1, source_dialect: string = '') =>
  client.post('/agents/sql-transpilation/transpile-ddl', { source_ddl, task_id, source_dialect }).then((r) => r.data)

export const transpileQuery = (source_sql: string, source_dialect: string = '') =>
  client.post('/agents/sql-transpilation/transpile-query', { source_sql, source_dialect }).then((r) => r.data)

export const transpileCTE = (source_sql: string, source_dialect: string = '') =>
  client.post('/agents/sql-transpilation/transpile-cte', { source_sql, source_dialect }).then((r) => r.data)

export const transpileWindow = (source_sql: string, source_dialect: string = '') =>
  client.post('/agents/sql-transpilation/transpile-window', { source_sql, source_dialect }).then((r) => r.data)

// Observability
export const getObservabilityTraces = (limit: number = 50) =>
  client.get('/agents/observability/traces', { params: { limit } }).then((r) => r.data)

export const getObservabilitySummary = () =>
  client.get('/agents/observability/summary').then((r) => r.data)
