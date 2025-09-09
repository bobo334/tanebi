import { Config } from '@/common/config';
import { MilkyApp } from '@/index';
import WebSocket from 'ws';

export class MilkyOneBotHandler {
    readonly logger;
    private clients = new Map<string, WebSocket>();
    private reconnectTimers = new Map<string, NodeJS.Timeout>();

    constructor(readonly app: MilkyApp, readonly config: Config['milky']['onebot']) {
        this.logger = app.logger.child({ module: 'OneBot' });
    }

    async start() {
        // Connect to all configured URLs
        for (const url of this.config.urls) {
            await this.connectToOneBot(url);
        }
    }

    private async connectToOneBot(url: string) {
        try {
            const ws = new WebSocket(url);
            
            ws.on('open', () => {
                this.logger.info(`Connected to OneBot reverse WebSocket: ${url}`);
                this.clients.set(url, ws);
                
                // Send initial heartbeat
                this.sendHeartbeat(ws);
                
                // Set up periodic heartbeat
                const heartbeatInterval = setInterval(() => {
                    this.sendHeartbeat(ws);
                }, this.config.heartbeatInterval || 5000);
                
                ws.on('close', () => {
                    clearInterval(heartbeatInterval);
                    this.logger.info(`Disconnected from OneBot reverse WebSocket: ${url}`);
                    this.clients.delete(url);
                    
                    // Schedule reconnection
                    if (!this.reconnectTimers.has(url)) {
                        const timer = setTimeout(() => {
                            this.reconnectTimers.delete(url);
                            this.connectToOneBot(url);
                        }, 3000);
                        this.reconnectTimers.set(url, timer);
                    }
                });
            });
            
            ws.on('error', (error: Error) => {
                this.logger.error(`Error in OneBot reverse WebSocket connection to ${url}: ${error}`);
            });
            
            ws.on('message', (data: WebSocket.Data) => {
                this.handleOneBotMessage(ws, data.toString());
            });
        } catch (error: unknown) {
            this.logger.error(`Failed to connect to OneBot reverse WebSocket ${url}: ${error}`);
        }
    }

    private sendHeartbeat(ws: WebSocket) {
        const heartbeat = {
            time: Math.floor(Date.now() / 1000),
            self_id: this.app.bot.uin,
            post_type: 'meta_event',
            meta_event_type: 'heartbeat',
            status: {
                online: this.app.bot.loggedIn,
                good: true
            },
            interval: this.config.heartbeatInterval || 5000
        };
        
        ws.send(JSON.stringify(heartbeat));
    }

    private async handleOneBotMessage(ws: WebSocket, message: string) {
        try {
            const data = JSON.parse(message);
            this.logger.debug(`Received OneBot message: ${message}`);
            
            // Handle API calls from OneBot clients
            if (data.action) {
                const response = await this.app.apiCollection.handle(data.action, data.params || {});
                const reply: Record<string, unknown> = {
                    echo: data.echo,
                    retcode: response.retcode,
                    status: response.retcode === 0 ? 'ok' : 'failed'
                };
                
                // Add data or message based on response type
                if (response.retcode === 0) {
                    reply.data = (response as { data: unknown }).data;
                } else {
                    reply.message = (response as { message: string }).message || 'API call failed';
                }
                
                ws.send(JSON.stringify(reply));
            }
        } catch (error: unknown) {
            this.logger.error(`Error handling OneBot message: ${error}`);
        }
    }

    async broadcast(msg: string) {
        for (const [url, ws] of this.clients.entries()) {
            if (ws.readyState === WebSocket.OPEN) {
                try {
                    ws.send(msg);
                } catch (error) {
                    this.logger.warn(`Failed to send message to OneBot client ${url}: ${error}`);
                }
            }
        }
    }

    stop() {
        // Clear all reconnection timers
        for (const timer of this.reconnectTimers.values()) {
            clearTimeout(timer);
        }
        this.reconnectTimers.clear();
        
        // Close all WebSocket connections
        for (const ws of this.clients.values()) {
            ws.close();
        }
        this.clients.clear();
    }
}