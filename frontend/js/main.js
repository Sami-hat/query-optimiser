/**
 * Main Application Script
 */

// Global instances
let flameGraph = null;
let heatmap = null;

// Last results — used by export functions
let lastQueryResult = null;
let lastBatchResult = null;

const EXAMPLE_QUERY = `SELECT u.username, u.email, o.total, o.created_at
FROM orders o
JOIN users u ON u.id = o.user_id
WHERE o.status = 'pending'
  AND o.created_at > '2024-01-01'
ORDER BY o.created_at DESC
LIMIT 50`;

// DOM elements
const statusDot = document.querySelector('.status-dot');
const statusText = document.querySelector('.status-text');
const queryInput = document.getElementById('query-input');
const analyzeBtn = document.getElementById('analyze-btn');
const exampleBtn = document.getElementById('example-btn');
const clearBtn = document.getElementById('clear-btn');
const analyzeCheck = document.getElementById('analyze-check');
const resultsSection = document.getElementById('results-section');
const loadingOverlay = document.getElementById('loading');

// Metrics elements
const execTimeEl = document.getElementById('exec-time');
const execTimeLabelEl = document.getElementById('exec-time-label');
const totalCostEl = document.getElementById('total-cost');
const rowsReturnedEl = document.getElementById('rows-returned');
const rowsLabelEl = document.getElementById('rows-label');
const seqScansEl = document.getElementById('seq-scans');

// Batch elements
const batchQueries = document.getElementById('batch-queries');
const batchAnalyzeBtn = document.getElementById('batch-analyze-btn');
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

    // Event listeners
    analyzeBtn.addEventListener('click', handleAnalyze);
    exampleBtn.addEventListener('click', () => { queryInput.value = EXAMPLE_QUERY; queryInput.focus(); });
    clearBtn.addEventListener('click', handleClear);
    batchAnalyzeBtn.addEventListener('click', handleBatchAnalyze);
    refreshTablesBtn.addEventListener('click', loadTableStats);

    document.getElementById('export-query-csv').addEventListener('click', exportQueryCSV);
    document.getElementById('export-query-pdf').addEventListener('click', exportQueryPDF);
    document.getElementById('export-batch-csv').addEventListener('click', exportBatchCSV);
    document.getElementById('export-batch-pdf').addEventListener('click', exportBatchPDF);

    // Ctrl+Enter to analyze
    queryInput.addEventListener('keydown', (e) => {
        if (e.ctrlKey && e.key === 'Enter') {
            handleAnalyze();
        }
    });

    await checkHealth();
    await loadTableStats();
});

// ===== Toast notifications =====

function toast(message, type = 'info', duration = 4500) {
    const container = document.getElementById('toast-container');
    const el = document.createElement('div');
    el.className = `toast toast-${type}`;
    el.textContent = message;
    container.appendChild(el);
    // trigger enter animation
    requestAnimationFrame(() => el.classList.add('show'));
    setTimeout(() => {
        el.classList.remove('show');
        el.addEventListener('transitionend', () => el.remove(), { once: true });
        setTimeout(() => el.remove(), 600); // fallback
    }, duration);
}

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

async function handleAnalyze() {
    const query = queryInput.value.trim();
    if (!query) {
        toast('Please enter a SQL query', 'warning');
        return;
    }

    showLoading(true);
    analyzeBtn.disabled = true;

    try {
        const result = await api.analyzeQuery(query, {
            includeExplain: true,
            analyze: analyzeCheck.checked
        });
        displayResults(result);
    } catch (error) {
        toast(`Analysis failed: ${error.message}`, 'error', 7000);
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

    // Update metrics. Without EXPLAIN ANALYZE there is no real execution time
    // and row counts are planner estimates.
    if (result.analyzed) {
        execTimeEl.textContent = result.metrics.execution_time_ms.toFixed(2);
        execTimeLabelEl.textContent = 'Execution Time (ms)';
        rowsLabelEl.textContent = 'Rows Returned';
    } else {
        execTimeEl.textContent = '—';
        execTimeLabelEl.textContent = 'Execution Time (estimate only)';
        rowsLabelEl.textContent = 'Rows (estimated)';
    }
    totalCostEl.textContent = result.metrics.total_cost.toFixed(2);
    rowsReturnedEl.textContent = result.metrics.actual_rows.toLocaleString();
    seqScansEl.textContent = result.sequential_scans.length;
    seqScansEl.classList.toggle('metric-bad', result.sequential_scans.length > 0);
    seqScansEl.classList.toggle('metric-good', result.sequential_scans.length === 0);

    // Render flame graph
    flameGraph.render(result.explain_plan);

    // Render recommendations
    displayRecommendations(result.recommendations, document.getElementById('recommendations'));

    // Show export buttons when there are recommendations
    const exportActions = document.getElementById('export-actions');
    exportActions.style.display =
        result.recommendations && result.recommendations.length > 0 ? 'flex' : 'none';

    // Render query rewrite suggestions
    displayRewrites(result.query_rewrites || []);

    resultsSection.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function displayRecommendations(recommendations, container) {
    container.innerHTML = '';

    if (!recommendations || recommendations.length === 0) {
        const p = document.createElement('p');
        p.className = 'no-recommendations';
        p.textContent = 'No index recommendations — query looks optimised!';
        container.appendChild(p);
        return;
    }

    recommendations.forEach((rec) => {
        const priority = rec.expected_improvement_pct >= 80 ? 'high-priority' :
                        rec.expected_improvement_pct >= 50 ? 'medium-priority' : '';
        const improvementClass = rec.expected_improvement_pct >= 80 ? 'improvement-high' : 'improvement-medium';

        const card = document.createElement('div');
        card.className = `recommendation-card ${priority}`;

        const header = document.createElement('div');
        header.className = 'recommendation-header';

        const tableSpan = document.createElement('span');
        tableSpan.className = 'recommendation-table';
        tableSpan.textContent = `${rec.table} (${rec.columns.join(', ')})`;

        header.appendChild(tableSpan);

        // JOIN-based recommendations have no cost estimate; skip the empty badge
        if (rec.expected_improvement_pct > 0) {
            const improvementSpan = document.createElement('span');
            improvementSpan.className = `recommendation-improvement ${improvementClass}`;
            improvementSpan.textContent = `+${rec.expected_improvement_pct.toFixed(0)}% improvement`;
            header.appendChild(improvementSpan);
        }

        const reason = document.createElement('div');
        reason.className = 'recommendation-reason';
        reason.textContent = rec.reason;

        const ddl = document.createElement('div');
        ddl.className = 'recommendation-ddl';
        ddl.textContent = rec.ddl;

        card.append(header, reason, ddl);

        if (rec.warning) {
            const warning = document.createElement('div');
            warning.className = 'recommendation-warning';
            warning.textContent = `⚠ ${rec.warning}`;
            card.appendChild(warning);
        }

        const actions = document.createElement('div');
        actions.className = 'recommendation-actions';

        const copyBtn = document.createElement('button');
        copyBtn.className = 'btn-copy';
        copyBtn.textContent = 'Copy DDL';
        copyBtn.addEventListener('click', () => copyToClipboard(copyBtn, rec.ddl));

        const applyBtn = document.createElement('button');
        applyBtn.className = 'btn-apply';
        applyBtn.textContent = 'Apply Index';
        applyBtn.addEventListener('click', () => applyRecommendation(applyBtn, rec));

        actions.append(copyBtn, applyBtn);
        card.appendChild(actions);

        container.appendChild(card);
    });
}

async function applyRecommendation(btn, rec) {
    const confirmed = window.confirm(
        `Create this index? It will be built with CREATE INDEX CONCURRENTLY ` +
        `(no table lock, but may take a while on large tables).\n\n${rec.ddl}`
    );
    if (!confirmed) return;

    btn.disabled = true;
    btn.textContent = 'Applying…';

    try {
        const response = await api.applyIndexes([rec.ddl], false);
        const result = response.results[0];
        if (result.success) {
            const secs = result.execution_time_ms != null
                ? ` in ${(result.execution_time_ms / 1000).toFixed(1)}s` : '';
            toast(`Index created${secs}. Re-analyze the query to see the effect.`, 'success', 7000);
            btn.textContent = 'Applied ✓';
            loadTableStats();
        } else {
            toast(`Index creation failed: ${result.error}`, 'error', 8000);
            btn.textContent = 'Apply Index';
            btn.disabled = false;
        }
    } catch (error) {
        toast(`Index creation failed: ${error.message}`, 'error', 8000);
        btn.textContent = 'Apply Index';
        btn.disabled = false;
    }
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
        tablesListEl.innerHTML = '';
        const p = document.createElement('p');
        p.className = 'load-error';
        p.textContent = `Failed to load table statistics: ${error.message}`;
        tablesListEl.appendChild(p);
    }
}

function displayTableCards(stats) {
    tablesListEl.innerHTML = '';

    stats.forEach(table => {
        const totalScans = (table.seq_scans || 0) + (table.index_scans || 0);
        const indexPct = totalScans > 0 ? ((table.index_scans || 0) / totalScans * 100) : 0;
        const usageClass = indexPct >= 80 ? 'stat-good' : indexPct >= 50 ? 'stat-warn' : 'stat-bad';

        const card = document.createElement('div');
        card.className = 'table-card';

        const rows = [
            ['Rows', (table.row_count || 0).toLocaleString(), ''],
            ['Size', table.total_size || '—', ''],
            ['Seq Scans', (table.seq_scans || 0).toLocaleString(), table.seq_scans > 0 ? 'stat-bad' : 'stat-muted'],
            ['Index Scans', (table.index_scans || 0).toLocaleString(), table.index_scans > 0 ? 'stat-good' : 'stat-muted'],
            ['Index Usage', totalScans > 0 ? `${indexPct.toFixed(1)}%` : '—', totalScans > 0 ? usageClass : 'stat-muted'],
            ['Write Ratio', `${(table.write_ratio * 100).toFixed(1)}%`, ''],
        ];

        const h4 = document.createElement('h4');
        h4.textContent = table.table_name;
        card.appendChild(h4);

        const grid = document.createElement('div');
        grid.className = 'table-stats';
        for (const [label, value, cls] of rows) {
            const stat = document.createElement('div');
            stat.className = 'table-stat';
            const labelSpan = document.createElement('span');
            labelSpan.className = 'table-stat-label';
            labelSpan.textContent = `${label}:`;
            const valueSpan = document.createElement('span');
            if (cls) valueSpan.className = cls;
            valueSpan.textContent = value;
            stat.append(labelSpan, valueSpan);
            grid.appendChild(stat);
        }
        card.appendChild(grid);
        tablesListEl.appendChild(card);
    });
}

async function handleBatchAnalyze() {
    const queriesText = batchQueries.value.trim();
    if (!queriesText) {
        toast('Please enter queries to analyze', 'warning');
        return;
    }

    const queries = queriesText.split('\n').map(q => q.trim()).filter(Boolean);
    if (queries.length === 0) {
        toast('No valid queries found', 'warning');
        return;
    }

    showLoading(true);
    batchAnalyzeBtn.disabled = true;

    try {
        const result = await api.batchAnalyze(queries, {
            maxWorkers: parseInt(workersInput.value) || 10,
            filterExisting: filterExistingCheck.checked
        });

        displayBatchResults(result);
    } catch (error) {
        toast(`Batch analysis failed: ${error.message}`, 'error', 7000);
    } finally {
        showLoading(false);
        batchAnalyzeBtn.disabled = false;
    }
}

function displayBatchResults(result) {
    lastBatchResult = result;
    batchResults.style.display = 'block';

    const summary = document.getElementById('batch-summary');
    summary.innerHTML = '';

    const heading = document.createElement('h3');
    heading.textContent = 'Analysis Summary';
    summary.appendChild(heading);

    const grid = document.createElement('div');
    grid.className = 'batch-summary-grid';

    const items = [
        [result.total_queries, 'Total Queries', ''],
        [result.analysed_queries, 'Analyzed', ''],
        [result.failed_queries, 'Failed', result.failed_queries > 0 ? 'stat-bad' : 'stat-good'],
        [result.total_seq_scans, 'Seq Scans', ''],
        [result.unique_recommendations, 'Recommendations', ''],
        [`${result.estimated_improvement_pct.toFixed(1)}%`, 'Est. Improvement', 'stat-good'],
        [`${result.analysis_duration_seconds.toFixed(2)}s`, 'Duration', ''],
    ];

    for (const [value, label, cls] of items) {
        const item = document.createElement('div');
        item.className = 'summary-item';
        const valueDiv = document.createElement('div');
        valueDiv.className = `summary-value ${cls}`.trim();
        valueDiv.textContent = value;
        const labelDiv = document.createElement('div');
        labelDiv.className = 'summary-label';
        labelDiv.textContent = label;
        item.append(valueDiv, labelDiv);
        grid.appendChild(item);
    }
    summary.appendChild(grid);

    // Render recommendations directly into the batch container
    displayRecommendations(result.top_recommendations, document.getElementById('batch-recommendations'));

    // Show batch export buttons
    const batchExport = document.getElementById('batch-export-actions');
    batchExport.style.display =
        result.top_recommendations && result.top_recommendations.length > 0 ? 'flex' : 'none';

    batchResults.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
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

        const card = document.createElement('div');
        card.className = 'rewrite-card';

        const header = document.createElement('div');
        header.className = 'rewrite-header';
        const name = document.createElement('span');
        name.className = 'rewrite-pattern-name';
        name.textContent = rw.description;
        const badge = document.createElement('span');
        badge.className = `improvement-badge ${badgeClass}`;
        badge.textContent = rw.improvement_level;
        header.append(name, badge);

        const reason = document.createElement('div');
        reason.className = 'rewrite-reason';
        reason.textContent = rw.reason;

        const snippetRow = document.createElement('div');
        snippetRow.className = 'rewrite-snippet-row';
        for (const [label, code, cls] of [
            ['Original', rw.original_snippet, 'rewrite-code original'],
            ['Suggested', rw.suggested_rewrite, 'rewrite-code'],
        ]) {
            const col = document.createElement('div');
            const labelDiv = document.createElement('div');
            labelDiv.className = 'rewrite-snippet-label';
            labelDiv.textContent = label;
            const codeDiv = document.createElement('div');
            codeDiv.className = cls;
            codeDiv.textContent = code;
            col.append(labelDiv, codeDiv);
            snippetRow.appendChild(col);
        }

        const actions = document.createElement('div');
        actions.className = 'rewrite-actions';

        const copySuggestionBtn = document.createElement('button');
        copySuggestionBtn.className = 'btn-copy';
        copySuggestionBtn.textContent = 'Copy suggestion';
        copySuggestionBtn.addEventListener('click', () => copyToClipboard(copySuggestionBtn, rw.suggested_rewrite));
        actions.appendChild(copySuggestionBtn);

        if (rw.rewritten_query) {
            const copyRewriteBtn = document.createElement('button');
            copyRewriteBtn.className = 'btn-copy';
            copyRewriteBtn.textContent = 'Copy full rewrite';
            copyRewriteBtn.addEventListener('click', () => copyToClipboard(copyRewriteBtn, rw.rewritten_query));
            actions.appendChild(copyRewriteBtn);
        }

        card.append(header, reason, snippetRow, actions);
        container.appendChild(card);
    });
}

function copyToClipboard(btn, text) {
    const markCopied = () => {
        const original = btn.textContent;
        btn.textContent = 'Copied!';
        btn.classList.add('copied');
        setTimeout(() => {
            btn.textContent = original;
            btn.classList.remove('copied');
        }, 1500);
    };

    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(markCopied).catch(() => fallbackCopy(text, markCopied));
    } else {
        fallbackCopy(text, markCopied);
    }
}

function fallbackCopy(text, onDone) {
    // Fallback for environments without the async clipboard API (e.g. plain http)
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    try {
        document.execCommand('copy');
        onDone();
    } finally {
        document.body.removeChild(ta);
    }
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
