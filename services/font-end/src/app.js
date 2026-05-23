/**
 * Whale Watch Dashboard — Main Application Logic
 * 
 * Kết nối WebSocket tới Dashboard API để nhận:
 * 1. Price updates (real-time từ Binance qua Kafka)
 * 2. Whale alerts (đã qua Spark xử lý)
 * 
 * Gọi REST API để load:
 * - Stats, history, configs
 */

const API_BASE = '';
const WS_BASE = `ws://${window.location.host}`;

// ─── STATE ───
const state = {
    prices: {},         // { BTCUSDT: { price: 67000, prev: 66950 } }
    alerts: [],         // Whale alerts list
    alertCountBySymbol: {},  // { BTCUSDT: 5, ETHUSDT: 3, ... }
    wsPrice: null,
    wsAlert: null,
    chart: null,
};

// ─── INIT ───
document.addEventListener('DOMContentLoaded', () => {
    startClock();
    loadStats();
    loadHistory();
    loadConfigs();
    initChart();
    connectWebSockets();
});

// ─── CLOCK ───
function startClock() {
    const el = document.getElementById('clock');
    setInterval(() => {
        el.textContent = new Date().toLocaleTimeString('vi-VN');
    }, 1000);
}

// ─── REST API CALLS ───
async function apiFetch(path) {
    try {
        const res = await fetch(`${API_BASE}${path}`);
        return await res.json();
    } catch (e) {
        console.error(`API error ${path}:`, e);
        return null;
    }
}

async function loadStats() {
    const data = await apiFetch('/api/alerts/stats');
    if (!data) return;
    document.getElementById('stat-alerts-value').textContent = data.total_alerts || 0;
    document.getElementById('stat-biggest-value').textContent = formatUSD(data.biggest_whale || 0);
    document.getElementById('stat-active-value').textContent = data.most_active_coin || 'N/A';
    document.getElementById('stat-volume-value').textContent = formatUSD(data.total_volume || 0);
}

async function loadHistory() {
    const data = await apiFetch('/api/alerts/history?limit=20');
    if (!data || !data.alerts) return;
    const tbody = document.getElementById('history-tbody');
    tbody.innerHTML = '';
    data.alerts.forEach(a => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${formatTime(a.alert_time)}</td>
            <td><span style="color:var(--accent-orange);font-weight:600">${a.symbol}</span></td>
            <td style="font-weight:600;color:var(--accent-green)">${formatUSD(a.total_usd)}</td>
        `;
        tbody.appendChild(tr);
    });
}

async function loadConfigs() {
    const data = await apiFetch('/api/configs');
    const tbody = document.getElementById('config-tbody');
    tbody.innerHTML = '';

    // Fallback nếu chưa có config trong DB
    const defaultThresholds = {
        BTCUSDT: 50000, ETHUSDT: 20000, BNBUSDT: 10000
    };

    const configs = (data && data.configs && data.configs.length > 0) 
        ? data.configs 
        : Object.entries(defaultThresholds).map(([s, t]) => ({ symbol: s, threshold_usd: t, is_active: true }));

    configs.forEach(cfg => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td style="font-weight:600;color:var(--accent-cyan)">${cfg.symbol}</td>
            <td>
                <input type="number" class="threshold-input" value="${cfg.threshold_usd}" 
                    onchange="updateThreshold('${cfg.symbol}', this.value)" />
            </td>
            <td>
                <label class="status-toggle">
                    <input type="checkbox" ${cfg.is_active ? 'checked' : ''}>
                    <span class="slider"></span>
                </label>
            </td>
        `;
        tbody.appendChild(tr);
    });
}

async function updateThreshold(symbol, value) {
    try {
        await fetch(`${API_BASE}/api/configs/${symbol}?threshold_usd=${value}`, { method: 'PUT' });
        console.log(`Updated ${symbol} → $${value}`);
    } catch (e) {
        console.error('Config update failed:', e);
    }
}

// ─── WEBSOCKET ───
function connectWebSockets() {
    connectPriceWS();
    connectAlertWS();
}

function connectPriceWS() {
    const ws = new WebSocket(`${WS_BASE}/ws/prices`);
    ws.onopen = () => { updateConnectionStatus(true); ws.send('ping'); };
    ws.onclose = () => { updateConnectionStatus(false); setTimeout(connectPriceWS, 3000); };
    ws.onerror = () => { updateConnectionStatus(false); };
    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data.type === 'price_update') handlePriceUpdate(data);
    };
    state.wsPrice = ws;
}

function connectAlertWS() {
    const ws = new WebSocket(`${WS_BASE}/ws/alerts`);
    ws.onopen = () => ws.send('ping');
    ws.onclose = () => setTimeout(connectAlertWS, 3000);
    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data.type === 'whale_alert') handleWhaleAlert(data);
    };
    state.wsAlert = ws;
}

function updateConnectionStatus(connected) {
    const el = document.getElementById('connection-status');
    const text = el.querySelector('.status-text');
    if (connected) {
        el.className = 'status-indicator online';
        text.textContent = 'LIVE';
    } else {
        el.className = 'status-indicator offline';
        text.textContent = 'Mất kết nối';
    }
}

// ─── HANDLERS ───
function handlePriceUpdate(data) {
    const { symbol, price } = data;
    const prev = state.prices[symbol]?.price || price;
    state.prices[symbol] = { price, prev };

    // Update ticker
    const priceEl = document.getElementById(`price-${symbol}`);
    if (priceEl) {
        priceEl.textContent = formatPrice(price);
        // Flash animation
        const card = document.getElementById(`ticker-${symbol}`);
        if (card) {
            card.classList.remove('flash-up', 'flash-down');
            void card.offsetWidth; // trigger reflow
            card.classList.add(price >= prev ? 'flash-up' : 'flash-down');
        }
    }
}

function handleWhaleAlert(data) {
    // Add to state
    state.alerts.unshift(data);
    if (state.alerts.length > 50) state.alerts.pop();

    // Update counter
    const sym = data.symbol;
    state.alertCountBySymbol[sym] = (state.alertCountBySymbol[sym] || 0) + 1;

    // Update badge
    document.getElementById('alert-count-badge').textContent = state.alerts.length;

    // Hide empty state
    const empty = document.getElementById('alerts-empty');
    if (empty) empty.style.display = 'none';

    // Add to feed
    const feed = document.getElementById('alerts-feed');
    const item = document.createElement('div');
    item.className = 'alert-item';
    item.innerHTML = `
        <span class="alert-icon">🚨</span>
        <div class="alert-info">
            <div><span class="alert-symbol">${data.symbol}</span></div>
            <div class="alert-value">${formatUSD(data.total_usd_value)}</div>
            <div class="alert-time">${data.window_start || new Date().toLocaleTimeString()}</div>
        </div>
    `;
    feed.insertBefore(item, feed.firstChild);

    // Limit feed items
    while (feed.children.length > 30) feed.removeChild(feed.lastChild);

    // Refresh stats
    loadStats();

    // Update chart
    updateChart();
}

// ─── CHART ───
function initChart() {
    const ctx = document.getElementById('activity-chart').getContext('2d');
    state.chart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['BTC', 'ETH', 'BNB'],
            datasets: [{
                label: 'Whale Alerts',
                data: [0, 0, 0],
                backgroundColor: [
                    'rgba(245,158,11,0.7)', 'rgba(139,92,246,0.7)', 'rgba(6,182,212,0.7)'
                ],
                borderColor: [
                    '#f59e0b', '#8b5cf6', '#06b6d4'
                ],
                borderWidth: 2,
                borderRadius: 6,
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                y: { beginAtZero: true, grid: { color: 'rgba(42,48,80,0.4)' }, ticks: { color: '#8892a8' } },
                x: { grid: { display: false }, ticks: { color: '#8892a8', font: { weight: '600' } } }
            }
        }
    });
}

function updateChart() {
    const symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT'];
    const counts = symbols.map(s => state.alertCountBySymbol[s] || 0);
    state.chart.data.datasets[0].data = counts;
    state.chart.update('none');
}

// ─── UTILS ───
function formatUSD(value) {
    if (!value || value === 0) return '$0';
    if (value >= 1000000) return `$${(value / 1000000).toFixed(2)}M`;
    if (value >= 1000) return `$${(value / 1000).toFixed(1)}K`;
    return `$${Number(value).toFixed(2)}`;
}

function formatPrice(price) {
    if (price >= 1000) return `$${Number(price).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2})}`;
    if (price >= 1) return `$${Number(price).toFixed(4)}`;
    return `$${Number(price).toFixed(6)}`;
}

function formatTime(isoString) {
    if (!isoString) return '--';
    const d = new Date(isoString);
    return d.toLocaleString('vi-VN', { hour:'2-digit', minute:'2-digit', second:'2-digit', day:'2-digit', month:'2-digit' });
}
