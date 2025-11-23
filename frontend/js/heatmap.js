/**
 * Index Coverage Heatmap
 * Visualises table scan patterns and index coverage
 */
class IndexHeatmap {
    constructor(containerId) {
        this.container = d3.select(`#${containerId}`);
        this.margin = { top: 30, right: 20, bottom: 50, left: 100 };
    }

    render(tableStats) {
        this.container.html('');

        if (!tableStats || tableStats.length === 0) {
            this.container.append('p')
                .style('color', '#aaa')
                .style('text-align', 'center')
                .text('No table statistics available');
            return;
        }

        const width = this.container.node().getBoundingClientRect().width - this.margin.left - this.margin.right;
        const barHeight = 40;
        const height = tableStats.length * barHeight;

        const svg = this.container.append('svg')
            .attr('width', width + this.margin.left + this.margin.right)
            .attr('height', height + this.margin.top + this.margin.bottom)
            .append('g')
            .attr('transform', `translate(${this.margin.left},${this.margin.top})`);

        // Calculate totals for scaling
        const maxScans = d3.max(tableStats, d => (d.seq_scans || 0) + (d.index_scans || 0));

        // Create scales
        const yScale = d3.scaleBand()
            .domain(tableStats.map(d => d.table_name))
            .range([0, height])
            .padding(0.2);

        const xScale = d3.scaleLinear()
            .domain([0, maxScans || 1])
            .range([0, width]);

        // Add Y axis (table names)
        svg.append('g')
            .call(d3.axisLeft(yScale))
            .selectAll('text')
            .style('fill', '#00d9ff')
            .style('font-size', '12px');

        // Draw stacked bars
        const barGroups = svg.selectAll('.bar-group')
            .data(tableStats)
            .enter()
            .append('g')
            .attr('class', 'bar-group')
            .attr('transform', d => `translate(0, ${yScale(d.table_name)})`);

        // Sequential scans (red)
        barGroups.append('rect')
            .attr('class', 'seq-scan-bar')
            .attr('x', 0)
            .attr('width', d => xScale(d.seq_scans || 0))
            .attr('height', yScale.bandwidth())
            .attr('fill', '#ff4444')
            .attr('rx', 3);

        // Index scans (green)
        barGroups.append('rect')
            .attr('class', 'index-scan-bar')
            .attr('x', d => xScale(d.seq_scans || 0))
            .attr('width', d => xScale(d.index_scans || 0))
            .attr('height', yScale.bandwidth())
            .attr('fill', '#00ff88')
            .attr('rx', 3);

        // Labels
        barGroups.append('text')
            .attr('x', d => xScale((d.seq_scans || 0) + (d.index_scans || 0)) + 5)
            .attr('y', yScale.bandwidth() / 2)
            .attr('dy', '0.35em')
            .style('fill', '#aaa')
            .style('font-size', '11px')
            .text(d => {
                const total = (d.seq_scans || 0) + (d.index_scans || 0);
                const indexPct = total > 0 ? ((d.index_scans || 0) / total * 100).toFixed(0) : 0;
                return `${indexPct}% indexed`;
            });

        // Legend
        const legend = svg.append('g')
            .attr('transform', `translate(0, ${height + 15})`);

        legend.append('rect')
            .attr('x', 0)
            .attr('width', 15)
            .attr('height', 15)
            .attr('fill', '#ff4444');

        legend.append('text')
            .attr('x', 20)
            .attr('y', 12)
            .style('fill', '#aaa')
            .style('font-size', '11px')
            .text('Sequential Scans');

        legend.append('rect')
            .attr('x', 130)
            .attr('width', 15)
            .attr('height', 15)
            .attr('fill', '#00ff88');

        legend.append('text')
            .attr('x', 150)
            .attr('y', 12)
            .style('fill', '#aaa')
            .style('font-size', '11px')
            .text('Index Scans');
    }
}
