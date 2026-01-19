/*
 * =================================================================================================
 * TRADINGVIEW DATA SERVER (V16 - 单源监控及API版)
 *
 * 这不是一个普通的文件。
 *
 * 为了实现“单文件独立运行”的目标，这个文件已经将 `tradingview-client-js` 库的
 * 所有核心源代码 (原位于 `src` 目录下) 全部内联到了文件顶部的 `TradingView` 模块中。
 *
 * 此版本提供稳定的 HTTP API，并内置两个启动任务：
 * 1. 一次性的历史K线下载测试，用于验证连接。
 * 2. 一个对单一数据源的持续实时订阅监控，具备自动重连功能。
 * =================================================================================================
 */

// --- 0. 外部依赖引入 ---
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { SocksProxyAgent } = require('socks-proxy-agent');

// =================================================================================================
// --- 核心库实现 (内联自 `tradingview-client-js/src`) ---
// =================================================================================================

const TradingView = (() => {
    const utils = {
        genSessionID(type = 'xs') {
            let r = '';
            const c = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
            for (let i = 0; i < 12; i += 1) r += c.charAt(Math.floor(Math.random() * c.length));
            return `${type}_${r}`;
        },
    };

    const protocol = {
        parseWSPacket(str) {
            const cleanerRgx = /~h~/g;
            const splitterRgx = /~m~[0-9]{1,}~m~/g;
            return str.replace(cleanerRgx, '').split(splitterRgx)
                .map((p) => {
                    if (!p) return false;
                    try { return JSON.parse(p); } 
                    catch (error) { console.warn('Cant parse', p); return false; }
                })
                .filter((p) => p);
        },
        formatWSPacket(packet) {
            const msg = typeof packet === 'object' ? JSON.stringify(packet) : packet;
            return `~m~${msg.length}~m~${msg}`;
        },
    };

    const chartSessionGenerator = (bridge) => class ChartSession {
        #chartSessionID = utils.genSessionID('cs');
        #bridge = bridge;
        #periods = {};
        get periods() { return Object.values(this.#periods).sort((a, b) => b.time - a.time); }
        #callbacks = { update: [], error: [] };

        #handleEvent(ev, ...data) { this.#callbacks[ev].forEach((e) => e(...data)); }
        #handleError(...msgs) {
            if (this.#callbacks.error.length === 0) console.error(...msgs);
            else this.#handleEvent('error', ...msgs);
        }

        constructor() {
            this.#bridge.sessions[this.#chartSessionID] = {
                type: 'chart',
                onData: (packet) => {
                           // --- 新增代码：打印完整的WebSocket数据包 ---
                    // 为了帮助您判断数据是完整的K线还是增量更新，我们在这里打印出收到的完整数据包。
                    // 您可以观察 'du' (data update) 类型的消息，看看 '$prices.s' 数组中包含的是什么。
                    // 如果每次 'du' 都包含完整的 [时间, 开, 高, 低, 收, 量]，那么它就是完整的K线。
                    // 如果只包含部分字段（例如只有价格和量），那么它可能是增量数据。
                    console.log('[原始WebSocket数据包]', JSON.stringify(packet, null, 2));
                    // --- 新增代码结束 ---
                    if (['timescale_update', 'du'].includes(packet.type)) {
                        const prices = packet.data[1].$prices;
                        if (!prices || !prices.s) return;
                        prices.s.forEach((p) => {
                            this.#periods[p.v[0]] = {
                                time: p.v[0], open: p.v[1], close: p.v[4],
                                max: p.v[2], min: p.v[3],
                                volume: Math.round(p.v[5] * 100) / 100,
                            };
                        });
                        this.#handleEvent('update');
                        return;
                    }
                    if (packet.type === 'symbol_error') this.#handleError(`(${packet.data[1]}) Symbol error:`, packet.data[2]);
                    if (packet.type === 'series_error') this.#handleError('Series error:', packet.data[3]);
                },
            };
            this.#bridge.send('chart_create_session', [this.#chartSessionID]);
        }

        #seriesCreated = false;
        #currentSeries = 0;

        setSeries(timeframe = '1', range = 100, to = null) {
            if (!this.#currentSeries) return this.#handleError('Please set market before series');
            this.#periods = {};
            this.#bridge.send(`${this.#seriesCreated ? 'modify' : 'create'}_series`, [
                this.#chartSessionID, '$prices', 's1', `ser_${this.#currentSeries}`,
                timeframe, this.#seriesCreated ? '' : (to ? ['bar_count', to, range] : range),
            ]);
            this.#seriesCreated = true;
        }

        setMarket(symbol, options = {}) {
            this.#periods = {};
            this.#currentSeries += 1;
            this.#bridge.send('resolve_symbol', [
                this.#chartSessionID, `ser_${this.#currentSeries}`,
                `=${JSON.stringify({ symbol: symbol || 'BTCEUR' })}`,
            ]);
            this.setSeries(options.timeframe, options.range, options.to);
        }
        
        onUpdate(cb) { this.#callbacks.update.push(cb); }
        onError(cb) { this.#callbacks.error.push(cb); }

        delete() {
            this.#bridge.send('chart_delete_session', [this.#chartSessionID]);
            delete this.#bridge.sessions[this.#chartSessionID];
        }
    };

    class Client {
        #ws;
        #logged = false;
        #sessions = {};
        #callbacks = { error: [] };
        #sourceName;
        
        #handleError(...msgs) {
            if (this.#callbacks.error.length === 0) console.error(...msgs);
            else this.#callbacks.error.forEach(cb => cb(...msgs));
        }
        
        #parsePacket(str) {
            protocol.parseWSPacket(str).forEach((packet) => {
                if (typeof packet === 'number') return this.#ws.send(protocol.formatWSPacket(`~h~${packet}`));
                if (packet.m === 'protocol_error') return this.#handleError(`[${this.#sourceName}] Client critical error:`, packet.p);
                if (packet.m && packet.p) {
                    const session = packet.p[0];
                    if (session && this.#sessions[session]) {
                        this.#sessions[session].onData({ type: packet.m, data: packet.p });
                    }
                }
            });
        }

        #sendQueue = [];
        send(t, p = []) {
            this.#sendQueue.push(protocol.formatWSPacket({ m: t, p }));
            this.sendQueue();
        }

        sendQueue() {
            while (this.#ws && this.#ws.readyState === WebSocket.OPEN && this.#logged && this.#sendQueue.length > 0) {
                this.#ws.send(this.#sendQueue.shift());
            }
        }
        
        get ws() {
            return this.#ws;
        }

        constructor(clientOptions = {}) {
            this.#sourceName = clientOptions.sourceName || 'Unknown Source';
            const websocketUrl = clientOptions.websocketUrl || 'wss://data.tradingview.com/socket.io/websocket?type=chart';
            console.log(`[${this.#sourceName}] Initializing connection to: ${websocketUrl}`);
            
            this.#ws = new WebSocket(websocketUrl, {
                origin: 'https://www.tradingview.com',
                agent: clientOptions.agent,
            });

            this.#sendQueue.unshift(protocol.formatWSPacket({
                m: 'set_auth_token', p: ['unauthorized_user_token'],
            }));
            this.#logged = true;
            
            this.#ws.on('open', () => {
                console.log(`[${this.#sourceName}] WebSocket connection established.`);
                this.sendQueue();
            });
            this.#ws.on('close', () => {
                console.warn(`[${this.#sourceName}] WebSocket connection closed.`);
            });
            this.#ws.on('error', (err) => this.#handleError(`[${this.#sourceName}] WebSocket error:`, err.message));
            this.#ws.on('message', (data) => this.#parsePacket(data.toString()));

            const clientBridge = {
                sessions: this.#sessions,
                send: (t, p) => this.send(t, p),
            };
            this.Session = { Chart: chartSessionGenerator(clientBridge) };
        }
        
        onError(cb) { this.#callbacks.error.push(cb); }
    }
    return { Client };
})();

// =================================================================================================
// --- 应用逻辑 (服务器代码) ---
// =================================================================================================

// --- 1. 配置部分 ---
const HTTP_PORT = 9002;
const PROXY_URL = 'socks://127.0.0.1:1080'; // 如果不需要代理，请设为 null
const SUPPORTED_SYMBOLS = [
  { "id": "BINANCE:BTCUSDT", "name": "比特币", "tick": 0.01 },
  { "id": "NASDAQ:NVDA", "name": "英伟达", "tick": 1.0 },
  { "id": "CAPITALCOM:US100", "name": "纳斯达克100指数", "tick": 0.1 }
];

// --- 2. 服务器启动流程 ---
async function startServer() {
    console.log('[Init] 正在设置代理...');
    const agent = PROXY_URL ? new SocksProxyAgent(PROXY_URL) : null;
    
    console.log('[Init] 正在为 HTTP API 和启动测试初始化 TradingView 客户端...');
    // 这个客户端用于处理一次性的 API 请求和启动时的测试
    const apiClient = new TradingView.Client({ agent, sourceName: 'API/Test Client' });
    apiClient.onError((...err) => console.error('[API Client Error]', ...err));
    
    // --- 3. 核心函数 ---
    const formatKline = (p, id, i) => ({
        symbol: id, interval: i, open_time: p.time * 1000,
        open: p.open, high: p.max, low: p.min, close: p.close, volume: p.volume,
    });

    // --- 4. HTTP API 服务器 ---
    const app = express();
    const server = http.createServer(app);

    app.get('/api/symbols', (req, res) => res.json(SUPPORTED_SYMBOLS));
    
    app.get('/api/klines/:symbol', (req, res) => {
      const { symbol } = req.params;
      const { period, count, to } = req.query;
      if (!period) return res.status(400).json({ error: "Period is required." });

      const chart = new apiClient.Session.Chart();
      chart.setMarket(symbol, {
        timeframe: period,
        range: count ? parseInt(count, 10) : 5000, 
        to: to ? Math.floor(parseInt(to, 10) / 1000) : undefined,
      });
      chart.onError((...err) => {
        if (!res.headersSent) res.status(500).json({ error: err[0] });
        chart.delete();
      });
      chart.onUpdate(() => {
        if (!chart.periods.length) return;
        res.json(chart.periods.map(p => formatKline(p, symbol, period)).reverse());
        chart.delete();
      });
    });

    // --- 5. WebSocket API 服务器 (占位) ---
    const wss = new WebSocket.Server({ server });
    // ... 可在此处实现您的 WebSocket 转发逻辑 ...

    // --- 6. 启动与内置任务 ---
    server.listen(HTTP_PORT, () => {
        console.log(`[+] 服务器已启动: http://localhost:${HTTP_PORT}`);
        runBuiltInTasks(apiClient, agent);
    });

    /**
     * 运行服务器启动后的内置任务，包括一次性测试和持续监控。
     * @param {TradingView.Client} clientForTest - 用于一次性测试的客户端实例
     * @param {SocksProxyAgent | null} agent - 代理配置
     */
    function runBuiltInTasks(clientForTest, agent) {
        console.log('\n--- [内置启动任务] ---');
        //const TEST_SYMBOL = 'BINANCE:ETHUSDT';
        const TEST_SYMBOL = 'CAPITALCOM:US500';


        // --- 任务 1: 下载历史K线进行快速测试 ---
        console.log(`\n[任务1] 正在获取 '${TEST_SYMBOL}' 最近5条1分钟历史K线...`);
        const historyChart = new clientForTest.Session.Chart();
        historyChart.onError((...err) => console.error(`[任务1] 错误:`, ...err));
        historyChart.setMarket(TEST_SYMBOL, { timeframe: '1', range: 5 });
        historyChart.onUpdate(() => {
            if (!historyChart.periods.length) return;
            console.log(`[任务1] 成功获取 ${historyChart.periods.length} 条历史K线:`);
            historyChart.periods.reverse().forEach(kline => {
                console.log(`  -> Time: ${new Date(kline.time * 1000).toLocaleString()}, C: ${kline.close}`);
            });
            historyChart.delete();
            console.log('[任务1] 历史K线测试完成。');
        });

        // --- 任务 2: 启动带守护重连的实时数据监控 ---
        console.log(`\n[任务2] 正在启动 '${TEST_SYMBOL}' 的实时更新监控...`);
        const REALTIME_SOURCE_URL = 'wss://prodata.tradingview.com/socket.io/websocket?type=chart';

        const createAndMonitor = () => {
            const sourceName = '实时监控源';
            console.log(`[监控] 正在订阅 '${TEST_SYMBOL}' 的实时更新...`);
            
            const monitorClient = new TradingView.Client({ 
                agent, 
                websocketUrl: REALTIME_SOURCE_URL, 
                sourceName 
            });

            const chart = new monitorClient.Session.Chart();
            chart.setMarket(TEST_SYMBOL, { timeframe: '1' });
            
            chart.onError((...err) => console.error(`[监控] Chart-level error:`, ...err));
            
            chart.onUpdate(() => {
                if (!chart.periods[0]) return;
                const k = chart.periods[0];
                console.log(`[监控] ${TEST_SYMBOL} -> C:${k.close} V:${k.volume} @ ${new Date(k.time*1000).toLocaleTimeString()}`);
            });

            monitorClient.ws.on('close', () => {
                console.warn(`[监控] 连接已断开。将在 5 秒后尝试重连...`);
                setTimeout(createAndMonitor, 5000);
            });

            monitorClient.onError((...err) => {
                console.error(`[监控] Client-level error:`, ...err);
            });
        };

        // 首次启动监控
        createAndMonitor();
    }
}

startServer().catch(err => console.error('[致命错误]', err));