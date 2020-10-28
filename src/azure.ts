import { AzureConfig } from './config';
import { sleep } from './utils/async';
import { createModuleDebug } from './utils/debug';
import { deepMerge, removeEmtpyValues } from './utils/obj';
import { exponentialBackoff } from './utils/retry';
import { AggregateMetric } from './utils/stats';
import { AbortController } from 'abort-controller';
import { EventGridPublisherClient, AzureKeyCredential } from '@azure/eventgrid';

const { debug, info, warn, error, trace } = createModuleDebug('azure');

/** Number of milliseconds since epoch */
export type EpochMillis = number;

export interface Metadata {
    host?: string;
    source?: string;
    sourcetype?: string;
    index?: string;
}

export type Fields = { [k: string]: any };

export interface Event {
    time: Date | EpochMillis;
    body: string | { [k: string]: any };
    fields?: Fields;
    metadata?: Metadata;
}

export interface AzureEvent {
    eventType: string;
    subject: string;
    dataVersion: string;
    data: Event;
}

export interface Metric {
    time: Date | EpochMillis;
    name: string;
    value: number;
    fields?: Fields;
    metadata?: Metadata;
}

export interface MultiMetrics {
    time: Date | EpochMillis;
    measurements: { [name: string]: number | undefined };
    fields?: Fields;
    metadata?: Metadata;
}

export function seralizeTime(time: Date | EpochMillis): number {
    if (time instanceof Date) {
        return +(time.getTime() / 1000).toFixed(3);
    }
    return +(time / 1000);
}

const CONFIG_DEFAULTS = {
    token: null,
    defaultMetadata: {},
    defaultFields: {},
    maxQueueEntries: -1,
    maxQueueSize: 521_000,
    flushTime: 0,
    gzip: true,
    maxRetries: Infinity,
    timeout: 30_000,
    requestKeepAlive: true,
    validatedCertificates: true,
    maxSockets: 256,
    userAgent: 'ethlogger-azure-client/1.0',
    retryWaitTime: exponentialBackoff({ min: 0, max: 180_000 }),
    multipleMetricFormatEnabled: false,
};

class FlushHandle {
    public promise: Promise<void> | null = null;

    constructor(private abortController: AbortController) {}

    public cancel() {
        this.abortController.abort();
    }
}

export function isMetric(msg: Event | Metric): msg is Metric {
    return 'name' in msg && typeof msg.name !== 'undefined';
}

export function parseAzureConfig(config: AzureConfig) {
    return {
        ...CONFIG_DEFAULTS,
        ...config,
    };
}

const initialCounters = {
    errorCount: 0,
    retryCount: 0,
    queuedMessages: 0,
    sentMessages: 0,
};

export class AzureClient {
    public readonly config: AzureConfig;
    private active: boolean = true;
    private queue: AzureEvent[] = [];
    private client: EventGridPublisherClient;

    private activeFlushing: Set<FlushHandle> = new Set();
    private flushTimer: NodeJS.Timer | null = null;

    private counters = {
        ...initialCounters,
    };
    private aggregates = {
        requestDuration: new AggregateMetric(),
        batchSize: new AggregateMetric(),
    };

    public constructor(config: AzureConfig) {
        this.config = parseAzureConfig(config);

        const keyCred = new AzureKeyCredential(config.key ?? '');
        this.client = new EventGridPublisherClient(config.url ?? '', keyCred);
    }

    public push(msg: Event | Metric) {
        return isMetric(msg) ? this.pushMetric(msg) : this.pushEvent(msg);
    }

    public pushEvent(event: Event) {
        this.queue.push({
            eventType: event.metadata?.sourcetype ?? 'ethlogger:transaction',
            subject: 'loggerdata',
            dataVersion: '1.0',
            data: event,
        });

        if (this.queue.length >= 25) {
            this.flushInternal();
        }
    }

    public pushMetric(metric: Metric) {
        /* no op */
    }

    public pushMetrics(metrics: MultiMetrics) {
        /* no op */
    }

    public flushStats() {
        const stats = {
            ...this.counters,
            ...this.aggregates.requestDuration.flush('requestDuration'),
            ...this.aggregates.batchSize.flush('batchSize'),
            activeFlushingCount: this.activeFlushing.size,
        };
        this.counters = { ...initialCounters };
        return stats;
    }

    public async flush(): Promise<void> {
        await Promise.all([...this.activeFlushing.values()].map(f => f.promise).concat(this.flushInternal()));
    }

    private flushInternal = (): Promise<void> => {
        if (this.flushTimer != null) {
            clearTimeout(this.flushTimer);
            this.flushTimer = null;
        }

        if (this.queue.length === 0) {
            return Promise.resolve();
        }

        const queue = this.queue;
        this.queue = [];

        const abortController = new AbortController();
        const flushHandle = new FlushHandle(abortController);
        const flushCompletePromise = this.sendToAzure(queue, abortController.signal);
        flushHandle.promise = flushCompletePromise;
        this.activeFlushing.add(flushHandle);

        const removeFromActive = () => this.activeFlushing.delete(flushHandle);
        flushCompletePromise.then(removeFromActive, removeFromActive);

        return flushCompletePromise;
    };

    private async sendToAzure(events: AzureEvent[], abortSignal: AbortSignal): Promise<void> {
        let attempt = 0;
        while (true) {
            attempt++;
            try {
                await this.client.sendEvents(events);
                info('%s items created', events.length);
                break;
            } catch (err) {
                error('Failed to send events to Azure successfully (attempt %s)', attempt, err.toString());

                if (abortSignal.aborted) {
                    throw new Error('Aborted');
                }

                if (attempt <= 10) {
                    await sleep(15000);
                    if (abortSignal.aborted) {
                        throw new Error('Aborted');
                    }
                    await this.client.sendEvents(events);
                }
            }
        }
    }

    public async shutdown(maxTime?: number) {
        info('Shutting down Azure client');
        this.active = false;

        if (maxTime != null && (this.activeFlushing.size > 0 || this.queue.length > 0)) {
            debug(`Waiting for ${this.activeFlushing.size} flush tasks to complete`);
            await Promise.race([sleep(maxTime), this.flush()]);
        }

        if (this.activeFlushing.size > 0) {
            debug(`Calling ${this.activeFlushing.size} flush tasks`);
            this.activeFlushing.forEach(f => f.cancel());
        }
    }
}
