/**
 * API Client for PostgreSQL Performance Analyzer
 *
 * Uses same-origin requests so it works both when served by the FastAPI app
 * (http://localhost:8000) and behind the nginx proxy (http://localhost).
 * Falls back to localhost:8000 when opened directly from the filesystem.
 */
class AnalyzerAPI {
    constructor(baseUrl = null) {
        if (baseUrl !== null) {
            this.baseUrl = baseUrl;
        } else if (window.location.protocol === 'file:') {
            this.baseUrl = 'http://localhost:8000';
        } else {
            this.baseUrl = '';
        }
        this.apiKey = null;
    }

    setApiKey(key) {
        this.apiKey = key;
    }

    async request(endpoint, options = {}) {
        const url = `${this.baseUrl}${endpoint}`;

        const headers = {
            'Content-Type': 'application/json',
            ...options.headers
        };

        if (this.apiKey) {
            headers['X-API-Key'] = this.apiKey;
        }

        const response = await fetch(url, {
            ...options,
            headers
        });

        if (!response.ok) {
            const error = await response.json().catch(() => ({ error: 'Unknown error' }));
            throw new Error(error.detail || error.error || `HTTP ${response.status}`);
        }

        return response.json();
    }

    async healthCheck() {
        return this.request('/health');
    }

    async analyzeQuery(query, { includeExplain = true, analyze = false } = {}) {
        return this.request('/analyse', {
            method: 'POST',
            body: JSON.stringify({
                query,
                include_explain: includeExplain,
                analyze
            })
        });
    }

    async batchAnalyze(queries, options = {}) {
        return this.request('/batch-analyse', {
            method: 'POST',
            body: JSON.stringify({
                queries,
                max_workers: options.maxWorkers || 10,
                filter_existing: options.filterExisting || false
            })
        });
    }

    async getTableStatistics() {
        return this.request('/tables');
    }

    async getTableRecommendations(tableName) {
        return this.request(`/recommendations/${encodeURIComponent(tableName)}`);
    }

    async applyIndexes(ddlStatements, dryRun = false) {
        return this.request('/apply-indexes', {
            method: 'POST',
            body: JSON.stringify({
                ddl_statements: ddlStatements,
                dry_run: dryRun
            })
        });
    }
}

// Global API instance
const api = new AnalyzerAPI();
