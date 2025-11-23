/**
 * API Client for PostgreSQL Performance Analyser
 */
class AnalyserAPI {
    constructor(baseUrl = 'http://localhost:8000') {
        this.baseUrl = baseUrl;
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

    async analyseQuery(query, includeExplain = true) {
        return this.request('/analyse', {
            method: 'POST',
            body: JSON.stringify({
                query,
                include_explain: includeExplain
            })
        });
    }

    async batchAnalyse(queries, options = {}) {
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
        return this.request(`/recommendations/${tableName}`);
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
const api = new AnalyserAPI();
