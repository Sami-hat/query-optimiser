/**
 * Main Application Script
 */

// Global instances
let flameGraph = null;
let heatmap = null;

// Last results — used by export functions
let lastQueryResult = null;
let lastBatchResult = null;

// DOM elements
const statusDot = document.querySelector('.status-dot');
const statusText = document.querySelector('.status-text');
const queryInput = document.getElementById('query-input');
const analyzeBtn = document.getElementById('analyze-btn');
const clearBtn = document.getElementById('clear-btn');
const resultsSection = document.getElementById('results-section');
const loadingOverlay = document.getElementById('loading');

// Metrics elements
const execTimeEl = document.getElementById('exec-time');
const totalCostEl = document.getElementById('total-cost');
const rowsReturnedEl = document.getElementById('rows-returned');
const seqScansEl = document.getElementById('seq-scans');

// Batch elements
const batchQueries = document.getElementById('batch-queries');
const batchanalyzeBtn = document.getElementById('batch-analyze-btn');
const workersInput = document.getElementById('workers');
const filterExistingCheck = document.getElementById('filter-existing');
const batchResults = document.getElementById('batch-results');

// Tables elements
const refreshTablesBtn = document.getElementById('refresh-tables');
const tablesListEl = document.getElementById('tables-list');

// Initialize
document.addEventListener('DOMContentLoaded', async () => {
    flameGraph = new FlameGraph('flamegraph');
    heatmap = new IndexHeatmap('tables-heatmap');

    await checkHealth();
    await loadTableStats();

    // Event listeners
    analyzeBtn.addEventListener('click', handleanalyze);
    clearBtn.addEventListener('click', handleClear);
    batchanalyzeBtn.addEventListener('click', handleBatchanalyze);
    refreshTablesBtn.addEventListener('click', loadTableStats);

    // Enter key to analyze
    queryInput.addEventListener('keydown', (e) => {
        if (e.ctrlKey && e.key === 'Enter') {
            handleanalyze();
        }
    });
});

async function checkHealth() {
    try {
        const health = await api.healthCheck();
        if (health.database_connected) {
            statusDot.classList.add('connected');
            statusDot.classList.remove('error');
            statusText.textContent = 'Connected';
        } else {
            statusDot.classList.add('error');
            statusDot.classList.remove('connected');
            statusText.textContent = 'Database disconnected';
        }
    } catch (error) {
        statusDot.classList.add('error');
        statusDot.classList.remove('connected');
        statusText.textContent = 'API unavailable';
    }
}

function showLoading(show = true) {
    loadingOverlay.style.display = show ? 'flex' : 'none';
}

async function handleanalyze() {
    const query = queryInput.value.trim();
    if (!query) {
        alert('Please enter a SQL query');
        return;
    }

    showLoading(true);
    analyzeBtn.disabled = true;

    try {
        const result = await api.analyzeQuery(query, true);
        displayResults(result);
    } catch (error) {
        alert(`analysis failed: ${error.message}`);
    } finally {
        showLoading(false);
        analyzeBtn.disabled = false;
    }
}

function handleClear() {
    queryInput.value = '';
    resultsSection.style.display = 'none';
    flameGraph.destroy();
}

function displayResults(result) {
    lastQueryResult = result;
    resultsSection.style.display = 'block';

    // Update metrics
    execTimeEl.textContent = result.metrics.execution_time_ms.toFixed(2);
    totalCostEl.textContent = result.metrics.total_cost.toFixed(2);
    rowsReturnedEl.textContent = result.metrics.actual_rows.toLocaleString();
    seqScansEl.textContent = result.sequential_scans.length;

    // Highlight if there are sequential scans
    if (result.sequential_scans.length > 0) {
        seqScansEl.style.color = '#ff4444';
    } else {
        seqScansEl.style.color = '#00ff88';
    }

    // Render flame graph
    flameGraph.render(result.explain_plan);

    // Render recommendations
    displayRecommendations(result.recommendations);

    // Show export buttons when there are recommendations
    const exportActions = document.getElementById('export-actions');
    if (exportActions) {
        exportActions.style.display = result.recommendations && result.recommendations.length > 0 ? 'flex' : 'none';
    }

    // Render query rewrite suggestions
    displayRewrites(result.query_rewrites || []);
}

function displayRecommendations(recommendations) {
    const container = document.getElementById('recommendations');
    container.innerHTML = '';

    if (!recommendations || recommendations.length === 0) {
        container.innerHTML = '<p style="color: #00ff88;">No index recommendations - query looks optimised!</p>';
        return;
    }

    recommendations.forEach((rec, index) => {
        const priority = rec.expected_improvement_pct >= 80 ? 'high-priority' :
                        rec.expected_improvement_pct >= 50 ? 'medium-priority' : '';
        const improvementClass = rec.expected_improvement_pct >= 80 ? 'improvement-high' : 'improvement-medium';

        const card = document.createElement('div');
        card.className = `recommendation-card ${priority}`;
        card.innerHTML = `
            <div class="recommendation-header">
                <span class="recommendation-table">${rec.table} (${rec.columns.join(', ')})</span>
                <span class="recommendation-improvement ${improvementClass}">
                    +${rec.expected_improvement_pct.toFixed(0)}% improvement
                </span>
            </div>
            <div class="recommendation-reason">${rec.reason}</div>
            <div class="recommendation-ddl">${rec.ddl}</div>
        `;
        container.appendChild(card);
    });
}

async function loadTableStats() {
    try {
        const stats = await api.getTableStatistics();

        // Render heatmap
        heatmap.render(stats);

        // Render table cards
        displayTableCards(stats);
    } catch (error) {
        console.error('Failed to load table stats:', error);
        tablesListEl.innerHTML = '<p style="color: #ff4444;">Failed to load table statistics</p>';
    }
}

function displayTableCards(stats) {
    tablesListEl.innerHTML = '';

    stats.forEach(table => {
        const totalScans = (table.seq_scans || 0) + (table.index_scans || 0);
        const indexPct = totalScans > 0 ? ((table.index_scans || 0) / totalScans * 100).toFixed(1) : '0';

        const card = document.createElement('div');
        card.className = 'table-card';
        card.innerHTML = `
            <h4>${table.table_name}</h4>
            <div class="table-stats">
                <div class="table-stat">
                    <span class="table-stat-label">Rows:</span>
                    <span>${(table.row_count || 0).toLocaleString()}</span>
                </div>
                <div class="table-stat">
                    <span class="table-stat-label">Size:</span>
                    <span>${table.total_size}</span>
                </div>
                <div class="table-stat">
                    <span class="table-stat-label">Seq Scans:</span>
                    <span style="color: ${table.seq_scans > 0 ? '#ff4444' : '#888'}">
                        ${(table.seq_scans || 0).toLocaleString()}
                    </span>
                </div>
                <div class="table-stat">
                    <span class="table-stat-label">Index Scans:</span>
                    <span style="color: ${table.index_scans > 0 ? '#00ff88' : '#888'}">
                        ${(table.index_scans || 0).toLocaleString()}
                    </span>
                </div>
                <div class="table-stat">
                    <span class="table-stat-label">Index Usage:</span>
                    <span style="color: ${parseFloat(indexPct) >= 80 ? '#00ff88' : parseFloat(indexPct) >= 50 ? '#ffaa00' : '#ff4444'}">
                        ${indexPct}%
                    </span>
                </div>
                <div class="table-stat">
                    <span class="table-stat-label">Write Ratio:</span>
                    <span>${(table.write_ratio * 100).toFixed(1)}%</span>
                </div>
            </div>
        `;
        tablesListEl.appendChild(card);
    });
}

async function handleBatchanalyze() {
    const queriesText = batchQueries.value.trim();
    if (!queriesText) {
        alert('Please enter queries to analyze');
        return;
    }

    const queries = queriesText.split('\n').filter(q => q.trim());
    if (queries.length === 0) {
        alert('No valid queries found');
        return;
    }

    showLoading(true);
    batchanalyzeBtn.disabled = true;

    try {
        const result = await api.batchanalyze(queries, {
            maxWorkers: parseInt(workersInput.value) || 10,
            filterExisting: filterExistingCheck.checked
        });

        displayBatchResults(result);
    } catch (error) {
        alert(`Batch analysis failed: ${error.message}`);
    } finally {
        showLoading(false);
        batchanalyzeBtn.disabled = false;
    }
}

function displayBatchResults(result) {
    lastBatchResult = result;
    batchResults.style.display = 'block';

    const summary = document.getElementById('batch-summary');
    summary.innerHTML = `
        <h3>Analysis Summary</h3>
        <div class="batch-summary-grid">
            <div class="summary-item">
                <div class="summary-value">${result.total_queries}</div>
                <div class="summary-label">Total Queries</div>
            </div>
            <div class="summary-item">
                <div class="summary-value">${result.analyzed_queries}</div>
                <div class="summary-label">analyzed</div>
            </div>
            <div class="summary-item">
                <div class="summary-value" style="color: ${result.failed_queries > 0 ? '#ff4444' : '#00ff88'}">
                    ${result.failed_queries}
                </div>
                <div class="summary-label">Failed</div>
            </div>
            <div class="summary-item">
                <div class="summary-value">${result.total_seq_scans}</div>
                <div class="summary-label">Seq Scans</div>
            </div>
            <div class="summary-item">
                <div class="summary-value">${result.unique_recommendations}</div>
                <div class="summary-label">Recommendations</div>
            </div>
            <div class="summary-item">
                <div class="summary-value" style="color: #00ff88">${result.estimated_improvement_pct.toFixed(1)}%</div>
                <div class="summary-label">Est. Improvement</div>
            </div>
            <div class="summary-item">
                <div class="summary-value">${result.analysis_duration_seconds.toFixed(2)}s</div>
                <div class="summary-label">Duration</div>
            </div>
        </div>
    `;

    const recommendations = document.getElementById('batch-recommendations');
    displayRecommendations(result.top_recommendations);

    // Copy recommendations to batch section
    recommendations.innerHTML = document.getElementById('recommendations').innerHTML;

    // Show batch export buttons
    const batchExport = document.getElementById('batch-export-actions');
    if (batchExport) {
        batchExport.style.display = result.top_recommendations && result.top_recommendations.length > 0 ? 'flex' : 'none';
    }
}

// Periodic health check
setInterval(checkHealth, 30000);

// ===== Query Rewrite Suggestions =====

function displayRewrites(rewrites) {
    const wrapper = document.getElementById('rewrites-container');
    const container = document.getElementById('query-rewrites');
    container.innerHTML = '';

    if (!rewrites || rewrites.length === 0) {
        wrapper.style.display = 'none';
        return;
    }

    wrapper.style.display = 'block';

    rewrites.forEach((rw) => {
        const badgeClass =
            rw.improvement_level === 'high' ? 'badge-high' :
            rw.improvement_level === 'medium' ? 'badge-medium' : 'badge-low';

        const copyRewriteBtn = rw.rewritten_query
            ? `<button class="btn-copy" onclick="copyToClipboard(this, ${JSON.stringify(rw.rewritten_query)})">Copy full rewrite</button>`
            : '';

        const card = document.createElement('div');
        card.className = 'rewrite-card';
        card.innerHTML = `
            <div class="rewrite-header">
                <span class="rewrite-pattern-name">${escapeHtml(rw.description)}</span>
                <span class="improvement-badge ${badgeClass}">${escapeHtml(rw.improvement_level)}</span>
            </div>
            <div class="rewrite-reason">${escapeHtml(rw.reason)}</div>
            <div class="rewrite-snippet-row">
                <div>
                    <div class="rewrite-snippet-label">Original</div>
                    <div class="rewrite-code original">${escapeHtml(rw.original_snippet)}</div>
                </div>
                <div>
                    <div class="rewrite-snippet-label">Suggested</div>
                    <div class="rewrite-code">${escapeHtml(rw.suggested_rewrite)}</div>
                </div>
            </div>
            <div class="rewrite-actions">
                <button class="btn-copy" onclick="copyToClipboard(this, ${JSON.stringify(rw.suggested_rewrite)})">Copy suggestion</button>
                ${copyRewriteBtn}
            </div>
        `;
        container.appendChild(card);
    });
}

function copyToClipboard(btn, text) {
    navigator.clipboard.writeText(text).then(() => {
        const original = btn.textContent;
        btn.textContent = 'Copied!';
        btn.classList.add('copied');
        setTimeout(() => {
            btn.textContent = original;
            btn.classList.remove('copied');
        }, 1500);
    }).catch(() => {
        // Fallback for environments without clipboard API
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
    });
}

function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.appendChild(document.createTextNode(str));
    return div.innerHTML;
}

// ===== CSV / PDF Export =====

function csvRow(...cells) {
    return cells.map(c => {
        const s = String(c == null ? '' : c).replace(/"/g, '""');
        return /[",\n]/.test(s) ? `"${s}"` : s;
    }).join(',');
}

function downloadFile(content, filename, mimeType) {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
}

function recsToCSV(recs, query) {
    const lines = [];
    if (query) {
        lines.push(csvRow('Query', query));
        lines.push('');
    }
    lines.push(csvRow('Table', 'Columns', 'Index Type', 'Expected Improvement %',
                       'Current Cost', 'Estimated Cost', 'Priority', 'Warning', 'DDL'));
    for (const r of recs) {
        lines.push(csvRow(
            r.table,
            (r.columns || []).join(', '),
            r.index_type,
            r.expected_improvement_pct != null ? r.expected_improvement_pct.toFixed(1) : '',
            r.current_cost != null ? r.current_cost.toFixed(2) : '',
            r.estimated_cost != null ? r.estimated_cost.toFixed(2) : '',
            r.priority,
            r.warning || '',
            r.ddl,
        ));
    }
    return lines.join('\n');
}

function exportQueryCSV() {
    if (!lastQueryResult || !lastQueryResult.recommendations.length) return;
    const csv = recsToCSV(lastQueryResult.recommendations, lastQueryResult.query);
    downloadFile(csv, 'index-recommendations.csv', 'text/csv');
}

function exportBatchCSV() {
    if (!lastBatchResult) return;
    const r = lastBatchResult;
    const lines = [];

    // Summary block
    lines.push(csvRow('Batch Analysis Summary'));
    lines.push(csvRow('Timestamp', r.timestamp));
    lines.push(csvRow('Total Queries', r.total_queries));
    lines.push(csvRow('Analysed', r.analysed_queries));
    lines.push(csvRow('Failed', r.failed_queries));
    lines.push(csvRow('Sequential Scans', r.total_seq_scans));
    lines.push(csvRow('Unique Recommendations', r.unique_recommendations));
    lines.push(csvRow('Estimated Improvement %', r.estimated_improvement_pct != null ? r.estimated_improvement_pct.toFixed(1) : ''));
    lines.push(csvRow('Tables Affected', (r.tables_affected || []).join('; ')));
    lines.push(csvRow('Analysis Duration (s)', r.analysis_duration_seconds != null ? r.analysis_duration_seconds.toFixed(2) : ''));
    lines.push('');

    // Top recommendations block
    lines.push(...recsToCSV(r.top_recommendations || []).split('\n'));

    downloadFile(lines.join('\n'), 'batch-analysis.csv', 'text/csv');
}

function exportQueryPDF() {
    if (!lastQueryResult) return;
    window.print();
}

function exportBatchPDF() {
    if (!lastBatchResult) return;
    window.print();
}
