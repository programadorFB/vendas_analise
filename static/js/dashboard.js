document.addEventListener('DOMContentLoaded', () => {
    // Objeto para armazenar as instâncias dos gráficos e dados
    const dashboardState = { charts: {}, currentData: null };

    // Mapeamento de todos os elementos da UI para fácil acesso
    const uiElements = {
        kpiSalesValue: document.getElementById('kpi-sales-value'),
        kpiAbandonedValue: document.getElementById('kpi-abandoned-value'),
        kpiRefundsValue: document.getElementById('kpi-refunds-value'),
        kpiSalesCount: document.getElementById('kpi-sales-count'),
        platformTableBody: document.getElementById('platform-table-body'),
        startDateInput: document.getElementById('start_date'),
        endDateInput: document.getElementById('end_date'),
        topNSelect: document.getElementById('top_n'),
        platformFilter: document.getElementById('platform_filter'),
        applyFiltersButton: document.getElementById('applyFilters'),
        loading: document.getElementById('loading'),
        dailyTrendChart: document.getElementById('dailyTrendChart'),
        platformRevenueChart: document.getElementById('platformRevenueChart'),
        topSellingChart: document.getElementById('topSellingChart'),
        topAbandonedChart: document.getElementById('topAbandonedChart'),
        refundsChart: document.getElementById('refundsChart'),
        exportPdfButton: document.getElementById('exportPdfButton'),
        exportDriveButton: document.getElementById('exportDriveButton'),
        refreshButton: document.getElementById('refreshButton')
    };

    const toggleLoading = (show) => { uiElements.loading.style.display = show ? 'flex' : 'none'; };

    // ==================== ALTERAÇÃO 1: SISTEMA DE NOTIFICAÇÕES ====================
    const showNotification = (message, type = 'info', duration = 3000) => {
        const notification = document.createElement('div');
        notification.className = `notification show ${type}`;
        notification.textContent = message;
        document.body.appendChild(notification);
        
        setTimeout(() => {
            notification.classList.remove('show');
            setTimeout(() => notification.remove(), 500);
        }, duration);
    };

    const renderChart = (ctx, type, data, options) => {
        const chartId = ctx.canvas.id;
        if (dashboardState.charts[chartId]) {
            dashboardState.charts[chartId].destroy();
        }
        dashboardState.charts[chartId] = new Chart(ctx, { type, data, options });
    };

    const fetchDashboardData = async () => {
        toggleLoading(true);
        const { startDateInput, endDateInput, topNSelect, platformFilter } = uiElements;
        const startDate = startDateInput.value;
        const endDate = endDateInput.value;
        const apiUrl = `/api/dashboard-data?start_date=${startDate}&end_date=${endDate}&top_n=${topNSelect.value}&platform=${platformFilter.value}`;

        try {
            const response = await fetch(apiUrl);
            if (!response.ok) throw new Error(`Erro na API: ${response.status}`);
            const data = await response.json();
            
            dashboardState.currentData = data;
            updateUI(data, topNSelect.value);
        } catch (error) {
            console.error("Falha ao buscar dados:", error);
            // ==================== ALTERAÇÃO 2: NOTIFICAÇÃO DE ERRO ====================
            showNotification('Erro ao carregar dados do dashboard', 'error');
        } finally {
            toggleLoading(false);
        }
    };
    
    const updateUI = (data, topN) => {
        // Atualiza os KPIs
        uiElements.kpiSalesValue.textContent = data.kpis.sales_value;
        uiElements.kpiAbandonedValue.textContent = data.kpis.abandoned_value;
        uiElements.kpiRefundsValue.textContent = data.kpis.refunds_value;
        uiElements.kpiSalesCount.textContent = data.kpis.total_sales;

        // Atualiza a Tabela de Plataformas
        uiElements.platformTableBody.innerHTML = '';
        if (data.platform_analysis.table_data.length > 0) {
            data.platform_analysis.table_data.forEach(row => {
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td><strong>${row.platform}</strong></td>
                    <td>${row.sales}</td>
                    <td>${row.profit}</td>
                    <td>${row.ticket}</td>
                    <td>${row.abandoned}</td>
                `;
                uiElements.platformTableBody.appendChild(tr);
            });
        }

        // Renderiza todos os gráficos
        renderAllCharts(data, topN);
    };

    const renderAllCharts = (data, topN) => {
        const commonOptions = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { labels: { color: '#334155' } },
                title: { display: true, font: { size: 16, weight: '600' }, color: '#1e293b', padding: { top: 10, bottom: 20 } }
            },
            scales: {
                x: { ticks: { color: '#64748b' }, grid: { display: false } },
                y: { ticks: { color: '#64748b' }, grid: { color: '#e2e8f0' } }
            }
        };

        // Renderiza os gráficos apenas se houver dados para eles
        if (data.daily_trend && data.daily_trend.labels.length > 0) {
            renderChart(uiElements.dailyTrendChart.getContext('2d'), 'line', {
                labels: data.daily_trend.labels,
                datasets: [{ label: 'Lucro', data: data.daily_trend.values, fill: true, backgroundColor: 'rgba(59, 130, 246, 0.1)', borderColor: '#3b82f6', tension: 0.3 }]
            }, { ...commonOptions, plugins: { ...commonOptions.plugins, title: { ...commonOptions.plugins.title, text: 'Tendência de Lucro Diário' }, legend: {display: false} } });
        }

        if (data.platform_analysis && data.platform_analysis.chart_labels.length > 0) {
            renderChart(uiElements.platformRevenueChart.getContext('2d'), 'doughnut', {
                labels: data.platform_analysis.chart_labels,
                datasets: [{ label: 'Lucro', data: data.platform_analysis.chart_values, backgroundColor: ['#3b82f6', '#10b981', '#f97316', '#8b5cf6', '#ef4444'] }]
            }, { responsive: true, plugins: { legend: { position: 'right' }, title: { ...commonOptions.plugins.title, text: 'Participação no Lucro' } } });
        }
        
        if (data.top_selling_products && data.top_selling_products.labels.length > 0) {
            renderChart(uiElements.topSellingChart.getContext('2d'), 'bar', {
                labels: data.top_selling_products.labels,
                datasets: [{ label: 'Vendas', data: data.top_selling_products.values, backgroundColor: '#10b981' }]
            }, { ...commonOptions, indexAxis: 'y', plugins: { ...commonOptions.plugins, title: { ...commonOptions.plugins.title, text: `Top ${topN} Produtos Mais Vendidos` }, legend: {display: false} } });
        }

        if (data.top_abandoned_products && data.top_abandoned_products.labels.length > 0) {
            renderChart(uiElements.topAbandonedChart.getContext('2d'), 'pie', {
                labels: data.top_abandoned_products.labels,
                datasets: [{ label: 'Abandonos', data: data.top_abandoned_products.values, backgroundColor: ['#f50b0bff', '#a34400ff', '#ea08c4ff', '#55f76bff', '#6366f1'] }]
            }, { responsive: true, plugins: { legend: { position: 'right' }, title: { ...commonOptions.plugins.title, text: `Top ${topN} Produtos com Abandono` } } });
        }
        
        if (data.refund_analysis && data.refund_analysis.labels.length > 0) {
            renderChart(uiElements.refundsChart.getContext('2d'), 'bar', {
                labels: data.refund_analysis.labels,
                datasets: [{ label: 'Reembolsos', data: data.refund_analysis.values, backgroundColor: '#ef4444' }]
            }, { ...commonOptions, indexAxis: 'y', plugins: { ...commonOptions.plugins, title: { ...commonOptions.plugins.title, text: 'Reembolsos por Plataforma' }, legend: {display: false} } });
        }
    };
    
    // ==================== ALTERAÇÃO 3: FUNÇÃO DE EXPORTAÇÃO PDF CORRIGIDA ====================
    const exportPDF = async () => {
        if (!dashboardState.currentData) { 
            showNotification('Nenhum dado disponível para exportar', 'warning');
            return; 
        }
        
        toggleLoading(true);
        const startDate = uiElements.startDateInput.value;
        const endDate = uiElements.endDateInput.value;
        const apiUrl = `/api/export-pdf?start_date=${startDate}&end_date=${endDate}`;
        
        try {
            showNotification('Gerando relatório PDF...', 'info');
            
            const response = await fetch(apiUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(dashboardState.currentData),
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || 'Falha ao gerar o PDF no servidor');
            }
            
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.style.display = 'none';
            a.href = url;
            a.download = `relatorio_dashboard_${new Date().toISOString().split('T')[0]}.pdf`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            a.remove();
            
            showNotification('PDF gerado e baixado com sucesso!', 'success');
        } catch (error) {
            console.error('Erro ao exportar PDF:', error);
            showNotification(`Erro ao exportar PDF: ${error.message}`, 'error');
        } finally {
            toggleLoading(false);
        }
    };

    // ==================== ALTERAÇÃO 4: FUNÇÃO DE EXPORTAÇÃO EXCEL CORRIGIDA ====================
    const exportToDrive = async () => { 
        if (!dashboardState.currentData) { 
            showNotification('Nenhum dado disponível para exportar', 'warning');
            return; 
        }
        
        toggleLoading(true);
        const startDate = uiElements.startDateInput.value;
        const endDate = uiElements.endDateInput.value;
        const platform = uiElements.platformFilter.value;
        
        // CORRIGIDO: URL para o endpoint correto de exportação Excel
        const apiUrl = `/api/export/excel?start_date=${startDate}&end_date=${endDate}&upload_drive=true${platform ? `&platform=${platform}` : ''}`;
        
        try {
            showNotification('Gerando planilha Excel e enviando para Google Drive...', 'info');
            
            const response = await fetch(apiUrl, {
                method: 'GET', // CORRIGIDO: Mudado para GET pois é assim que está implementado
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Falha ao gerar Excel: ${response.status} - ${errorText}`);
            }
            
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.style.display = 'none';
            a.href = url;
            a.download = `relatorio_dashboard_${new Date().toISOString().split('T')[0]}.xlsx`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            a.remove();
            
            showNotification('Excel gerado, baixado e enviado para Google Drive!', 'success');
        } catch (error) {
            console.error('Erro ao exportar Excel:', error);
            showNotification(`Erro ao exportar Excel: ${error.message}`, 'error');
        } finally {
            toggleLoading(false);
        }
    }; 
    
    // ==================== ALTERAÇÃO 5: FUNÇÃO DE REFRESH COM NOTIFICAÇÃO ====================
    const refreshData = () => {
        showNotification('Atualizando dados...', 'info');
        fetchDashboardData();
    };
    
    const init = () => {
        const today = new Date();
        const thirtyDaysAgo = new Date(new Date().setDate(today.getDate() - 29));
        uiElements.endDateInput.value = today.toISOString().split('T')[0];
        uiElements.startDateInput.value = thirtyDaysAgo.toISOString().split('T')[0];
        
        uiElements.applyFiltersButton.addEventListener('click', fetchDashboardData);
        uiElements.exportPdfButton.addEventListener('click', exportPDF);
        uiElements.exportDriveButton.addEventListener('click', exportToDrive);
        uiElements.refreshButton.addEventListener('click', refreshData);
        
        fetchDashboardData();
    };

    init();
});