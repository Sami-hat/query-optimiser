/**
 * Flame Graph Visualisation for EXPLAIN Plans
 * Uses D3.js to render hierarchical execution plans
 */
class FlameGraph {
    constructor(containerId) {
        this.container = d3.select(`#${containerId}`);
        this.margin = { top: 20, right: 20, bottom: 30, left: 20 };
        this.tooltip = null;
        this.colorScale = this.createColorScale();
    }

    createColorScale() {
        return {
            'Seq Scan': '#ff4444',
            'Index Scan': '#00ff88',
            'Index Only Scan': '#00dd66',
            'Bitmap Heap Scan': '#88cc00',
            'Bitmap Index Scan': '#66aa00',
            'Nested Loop': '#ffaa00',
            'Hash Join': '#ff8800',
            'Merge Join': '#ff6600',
            'Hash': '#aa88ff',
            'Sort': '#8866ff',
            'Aggregate': '#6644ff',
            'Gather': '#00d9ff',
            'Gather Merge': '#00b8d9',
            'Result': '#888888',
            'Limit': '#aaaaaa',
            'default': '#666666'
        };
    }

    getNodeColor(nodeType) {
        return this.colorScale[nodeType] || this.colorScale.default;
    }

    render(explainPlan) {
        this.container.html('');

        if (!explainPlan || !explainPlan.explain_plan || !explainPlan.explain_plan.Plan) {
            this.container.append('p')
                .attr('class', 'no-data')
                .style('color', '#aaa')
                .style('text-align', 'center')
                .style('padding', '20px')
                .text('No execution plan data available');
            return;
        }

        const plan = explainPlan.explain_plan.Plan;
        const width = this.container.node().getBoundingClientRect().width - this.margin.left - this.margin.right;
        const hierarchy = this.buildHierarchy(plan);
        const height = this.calculateHeight(hierarchy);

        const svg = this.container.append('svg')
            .attr('width', width + this.margin.left + this.margin.right)
            .attr('height', height + this.margin.top + this.margin.bottom)
            .append('g')
            .attr('transform', `translate(${this.margin.left},${this.margin.top})`);

        // Create tooltip
        this.tooltip = d3.select('body').append('div')
            .attr('class', 'flamegraph-tooltip')
            .style('opacity', 0);

        this.drawFlameGraph(svg, hierarchy, width, height);
    }

    buildHierarchy(plan, parent = null) {
        const node = {
            name: plan['Node Type'],
            cost: plan['Total Cost'] || 0,
            time: plan['Actual Total Time'] || 0,
            rows: plan['Actual Rows'] || plan['Plan Rows'] || 0,
            details: {
                table: plan['Relation Name'],
                filter: plan['Filter'],
                indexName: plan['Index Name'],
                startupCost: plan['Startup Cost'],
                rowsRemoved: plan['Rows Removed by Filter']
            },
            children: []
        };

        if (plan.Plans) {
            node.children = plan.Plans.map(child => this.buildHierarchy(child, node));
        }

        return node;
    }

    calculateHeight(hierarchy) {
        const levels = this.countLevels(hierarchy);
        return Math.max(200, levels * 50 + 50);
    }

    countLevels(node, level = 0) {
        if (!node.children || node.children.length === 0) {
            return level + 1;
        }
        return Math.max(...node.children.map(child => this.countLevels(child, level + 1)));
    }

    drawFlameGraph(svg, root, width, height) {
        const nodes = this.flattenHierarchy(root, 0, 0, width);
        const barHeight = 35;
        const padding = 2;

        // Draw nodes
        const nodeGroups = svg.selectAll('.flamegraph-node')
            .data(nodes)
            .enter()
            .append('g')
            .attr('class', 'flamegraph-node')
            .attr('transform', d => `translate(${d.x}, ${height - (d.level + 1) * (barHeight + padding)})`);

        // Rectangles
        nodeGroups.append('rect')
            .attr('width', d => Math.max(d.width - 2, 1))
            .attr('height', barHeight)
            .attr('rx', 3)
            .attr('fill', d => this.getNodeColor(d.name))
            .attr('stroke', '#1a1a2e')
            .attr('stroke-width', 1)
            .on('mouseover', (event, d) => this.showTooltip(event, d))
            .on('mouseout', () => this.hideTooltip());

        // Labels
        nodeGroups.append('text')
            .attr('x', d => Math.min(d.width / 2, 100))
            .attr('y', barHeight / 2)
            .attr('dy', '0.35em')
            .attr('text-anchor', d => d.width > 80 ? 'middle' : 'start')
            .attr('fill', '#fff')
            .attr('font-size', '11px')
            .attr('font-weight', 'bold')
            .text(d => {
                if (d.width < 50) return '';
                const maxLen = Math.floor(d.width / 7);
                const text = d.name;
                return text.length > maxLen ? text.substring(0, maxLen - 1) + '...' : text;
            });

        // Time labels
        nodeGroups.append('text')
            .attr('x', d => d.width - 5)
            .attr('y', barHeight / 2)
            .attr('dy', '0.35em')
            .attr('text-anchor', 'end')
            .attr('fill', 'rgba(255,255,255,0.7)')
            .attr('font-size', '10px')
            .text(d => d.width > 100 ? `${d.time.toFixed(1)}ms` : '');
    }

    flattenHierarchy(node, level, x, width) {
        const result = [];

        result.push({
            ...node,
            level,
            x,
            width
        });

        if (node.children && node.children.length > 0) {
            // Calculate child widths based on their costs
            const totalChildCost = node.children.reduce((sum, c) => sum + (c.cost || 1), 0);
            let currentX = x;

            node.children.forEach(child => {
                const childWidth = (child.cost / totalChildCost) * width;
                result.push(...this.flattenHierarchy(child, level + 1, currentX, childWidth));
                currentX += childWidth;
            });
        }

        return result;
    }

    showTooltip(event, d) {
        const content = `
            <div class="title">${d.name}</div>
            <div>Time: ${d.time.toFixed(2)} ms</div>
            <div>Cost: ${d.cost.toFixed(2)}</div>
            <div>Rows: ${d.rows.toLocaleString()}</div>
            ${d.details.table ? `<div>Table: ${d.details.table}</div>` : ''}
            ${d.details.filter ? `<div>Filter: ${d.details.filter}</div>` : ''}
            ${d.details.rowsRemoved ? `<div>Rows Filtered: ${d.details.rowsRemoved.toLocaleString()}</div>` : ''}
            ${d.details.indexName ? `<div>Index: ${d.details.indexName}</div>` : ''}
        `;

        this.tooltip
            .style('opacity', 1)
            .html(content)
            .style('left', (event.pageX + 10) + 'px')
            .style('top', (event.pageY - 10) + 'px');
    }

    hideTooltip() {
        this.tooltip.style('opacity', 0);
    }

    destroy() {
        if (this.tooltip) {
            this.tooltip.remove();
        }
        this.container.html('');
    }
}
