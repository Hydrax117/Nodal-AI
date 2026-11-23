/**
 * backend/triggers.ts
 *
 * Event-driven triggers for autonomous PayFi.
 * "when X arrives from Y, do Z" - persisted and deduplicated.
 */

import { PayFiAgent, AgentTask } from './agent';
import { config } from './config';
import { createLogger } from './utils/logger';
import { getDb } from './persistence';
import { toStroops, stroopsToString } from './utils/amount';
import { listen as listenContractEvents } from './tools/ContractEventListener';
import { watchContractStorage, ContractStorageWatcherOptions } from './tools/ContractStorageWatcherTool';
import { xdr } from '@stellar/stellar-sdk';

// Re-export for testing
export { listenContractEvents, watchContractStorage };

const log = createLogger('triggers');

export type TriggerSource = 'payment' | 'contract_event' | 'storage_change';

export interface TriggerFilter {
  /** Filter by source asset code (e.g., 'USDC') */
  assetCode?: string;
  /** Filter by asset issuer (required for non-XLM assets) */
  assetIssuer?: string;
  /** Filter by sender public key */
  from?: string;
  /** Filter by amount threshold */
  minAmount?: string;
  /** Filter by contract ID */
  contractId?: string;
  /** Filter by event topics */
  eventTopics?: string[];
  /** Filter by storage key */
  storageKey?: string;
}

export interface Trigger {
  id: string;
  source: TriggerSource;
  filter: TriggerFilter;
  /** Task to execute when trigger fires */
  task: AgentTask;
  enabled: boolean;
  createdAt: string;
  lastFiredAt?: string;
}

/**
 * Persist trigger to database.
 */
async function saveTrigger(trigger: Trigger): Promise<void> {
  getDb()
    .prepare(
      `INSERT OR REPLACE INTO triggers (id, source, filter, task, enabled, created_at, last_fired_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    )
    .run(
      trigger.id,
      trigger.source,
      JSON.stringify(trigger.filter),
      JSON.stringify(trigger.task),
      trigger.enabled ? 1 : 0,
      trigger.createdAt,
      trigger.lastFiredAt ?? null
    );
}

/**
 * Load all triggers from database.
 */
async function loadTriggers(): Promise<Trigger[]> {
  try {
    const rows = getDb().prepare('SELECT * FROM triggers').all() as Array<{
      id: string;
      source: string;
      filter: string;
      task: string;
      enabled: number;
      created_at: string;
      last_fired_at: string | null;
    }>;

    return rows.map((row) => ({
      id: row.id,
      source: row.source as TriggerSource,
      filter: JSON.parse(row.filter),
      task: JSON.parse(row.task),
      enabled: row.enabled === 1,
      createdAt: row.created_at,
      ...(row.last_fired_at !== null ? { lastFiredAt: row.last_fired_at } : {}),
    }));
  } catch {
    // Table doesn't exist yet
    return [];
  }
}

/**
 * Record that a trigger fired for an event (dedupe).
 */
async function recordTriggerFired(triggerId: string, eventId: string): Promise<boolean> {
  const key = `trigger:${triggerId}:event:${eventId}`;
  // One atomic statement: of two concurrent deliveries of the same event only
  // one inserts the row, so a check-then-insert race cannot fire it twice.
  const info = getDb()
    .prepare('INSERT OR IGNORE INTO trigger_events (key, fired_at) VALUES (?, ?)')
    .run(key, new Date().toISOString());
  return info.changes === 1; // 0 = already fired for this event
}

/**
 * Check if event matches trigger filter.
 */
export function matchesFilter(event: unknown, filter: TriggerFilter): boolean {
  const e = event as Record<string, unknown>;

  if (filter.assetCode) {
    const eventAssetCode = (e.assetCode as string) ?? (e.asset as string);
    // For non-XLM assets, require assetIssuer match too
    if (filter.assetCode !== 'XLM' && filter.assetIssuer) {
      const eventAssetIssuer = (e.assetIssuer as string) ?? '';
      if (eventAssetCode !== filter.assetCode || eventAssetIssuer !== filter.assetIssuer) {
        return false;
      }
    } else {
      // For XLM or when no issuer specified, match only on code
      if (eventAssetCode !== filter.assetCode) return false;
    }
  }

  if (filter.from) {
    if (e.from !== filter.from && e.source !== filter.from) return false;
  }

  if (filter.minAmount) {
    const eventAmount = e.amount ?? e.value ?? '0';
    const minAmount = filter.minAmount;
    // Use BigInt stroop comparison to avoid float precision issues
    try {
      const eventStroops = toStroops(String(eventAmount));
      const minStroops = toStroops(minAmount);
      if (eventStroops < minStroops) return false;
    } catch {
      // If amount is invalid, treat as 0
      return false;
    }
  }

  if (filter.contractId) {
    if (e.contractId !== filter.contractId) return false;
  }

  if (filter.eventTopics?.length) {
    const eventTopics = (e.topics as string[]) ?? [];
    if (!filter.eventTopics.some((t) => eventTopics.includes(t))) return false;
  }

  return true;
}

/**
 * TriggerManager - handles event-driven automation.
 */
export class TriggerManager {
  private triggers: Map<string, Trigger> = new Map();
  private agent: PayFiAgent;
  private unlistenPayment?: () => void;
  private unlistenContract?: () => void;
  private unlistenStorage?: () => void;
  private contractWatchers = new Map<string, { stop: () => void; triggers: Set<string> }>();
  private storageWatchers = new Map<string, { stop: () => void; triggers: Set<string> }>();

  constructor(agent: PayFiAgent) {
    this.agent = agent;
  }

  /**
   * Load triggers from database and start listening.
   */
  async start(): Promise<void> {
    const loaded = await loadTriggers();
    for (const trigger of loaded) {
      this.triggers.set(trigger.id, trigger);
    }
    log.info({ count: this.triggers.size }, 'Triggers loaded');

    // Set up listeners
    this.setupPaymentListener();
    this.setupContractListener();
    this.setupStorageListener();
  }

  private setupPaymentListener(): void {
    this.agent.startListening('trigger', async (challenge) => {
      const payment = {
        assetCode: challenge.assetCode,
        assetIssuer: challenge.assetIssuer,
        from: challenge.payTo, // Incoming payment
        amount: challenge.amount,
      };

      for (const [id, trigger] of this.triggers) {
        if (!trigger.enabled || trigger.source !== 'payment') continue;
        if (!matchesFilter(payment, trigger.filter)) continue;

        // Dedupe by event
        const eventId = challenge.nonce;
        const isNew = await recordTriggerFired(id, eventId);
        if (!isNew) continue;

        // Check spending limit before execution
        const p = trigger.task.payload as Record<string, unknown>;
        if (p.amount) {
          const limit = parseFloat(config.AGENT_SPENDING_LIMIT);
          const amount = parseFloat(String(p.amount));
          if (amount > limit) {
            log.warn({ triggerId: id, amount }, 'Trigger exceeds spending limit');
            continue;
          }
        }

        log.info({ triggerId: id, eventId }, 'Trigger firing');
        await this.agent.run(trigger.task);

        // Update last fired
        trigger.lastFiredAt = new Date().toISOString();
        await saveTrigger(trigger);
      }
    });
  }

  private setupContractListener(): void {
    // Group triggers by contractId
    const contractTriggers = new Map<string, Trigger[]>();

    for (const [id, trigger] of this.triggers) {
      if (!trigger.enabled || trigger.source !== 'contract_event') continue;
      if (!trigger.filter.contractId) continue;
      const triggers = contractTriggers.get(trigger.filter.contractId) ?? [];
      triggers.push(trigger);
      contractTriggers.set(trigger.filter.contractId, triggers);
    }

    // Set up a listener for each contractId with contract_event triggers
    for (const [contractId, triggers] of contractTriggers) {
      const eventTypes = triggers
        .map((t) => t.filter.eventTopics ?? [])
        .flat()
        .filter((v, i, a) => a.indexOf(v) === i); // dedupe

      const handle = listenContractEvents(
        contractId,
        eventTypes,
        (event) => {
          this.handleContractEvent(contractId, event, triggers);
        },
        { maxReconnectAttempts: 5 }
      );

      this.contractWatchers.set(contractId, {
        stop: () => {
          handle.stop();
          handle.removeAllListeners();
        },
        triggers: new Set(triggers.map((t) => t.id)),
      });
    }

    this.unlistenContract = () => {
      for (const { stop } of this.contractWatchers.values()) {
        stop();
      }
      this.contractWatchers.clear();
    };
  }

  private setupStorageListener(): void {
    // Group triggers by contractId
    const contractOptions = new Map<string, ContractStorageWatcherOptions>();

    for (const [id, trigger] of this.triggers) {
      if (!trigger.enabled || trigger.source !== 'storage_change') continue;
      if (!trigger.filter.contractId) continue;
      if (!trigger.filter.storageKey) continue;

      const contractId = trigger.filter.contractId;
      const options = contractOptions.get(contractId);

      if (options) {
        options.keys.push(trigger.filter.storageKey);
      } else {
        // Parse storageKey string to xdr.LedgerKey
        let keys: (xdr.LedgerKey | xdr.ScVal | string)[] = [];
        try {
          keys.push(xdr.LedgerKey.fromXDR(trigger.filter.storageKey, 'base64'));
        } catch {
          try {
            const scVal = xdr.ScVal.fromXDR(trigger.filter.storageKey, 'base64');
            keys.push(scVal);
          } catch {
            keys.push(trigger.filter.storageKey);
          }
        }

        contractOptions.set(contractId, {
          contractId,
          keys,
          pollIntervalMs: config.CONTRACT_EVENT_POLL_MS || 5000,
        });
      }
    }

    // Set up a watcher for each contractId with storage_change triggers
    for (const [contractId, options] of contractOptions) {
      const handle = watchContractStorage(options);

      handle.on('change', (event) => {
        this.handleStorageChange(contractId, event);
      });

      this.storageWatchers.set(contractId, {
        stop: () => {
          handle.stop();
          handle.removeAllListeners();
        },
        triggers: new Set(
          Array.from(this.triggers.values())
            .filter((t) => t.enabled && t.source === 'storage_change' && t.filter.contractId === contractId)
            .map((t) => t.id)
        ),
      });
    }

    this.unlistenStorage = () => {
      for (const { stop } of this.storageWatchers.values()) {
        stop();
      }
      this.storageWatchers.clear();
    };
  }

  /**
   * Create a new trigger.
   */
  async createTrigger(options: {
    source: TriggerSource;
    filter: TriggerFilter;
    task: AgentTask;
  }): Promise<Trigger> {
    const { filter } = options;

    // Validate that non-native assets have assetIssuer specified
    if (filter.assetCode && filter.assetCode !== 'XLM' && !filter.assetIssuer) {
      throw new Error(
        `assetIssuer is required for non-XLM asset trigger (assetCode: ${filter.assetCode})`
      );
    }

    const trigger: Trigger = {
      id: `trig_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`,
      source: options.source,
      filter: options.filter,
      task: options.task,
      enabled: true,
      createdAt: new Date().toISOString(),
    };

    this.triggers.set(trigger.id, trigger);
    await saveTrigger(trigger);

    log.info({ triggerId: trigger.id, source: trigger.source }, 'Trigger created');
    return trigger;
  }

  /**
   * Enable/disable a trigger.
   */
  async setTriggerEnabled(id: string, enabled: boolean): Promise<void> {
    const trigger = this.triggers.get(id);
    if (!trigger) throw new Error(`Trigger not found: ${id}`);

    trigger.enabled = enabled;
    await saveTrigger(trigger);
    log.info({ triggerId: id, enabled }, 'Trigger updated');
  }

  /**
   * Delete a trigger.
   */
  async deleteTrigger(id: string): Promise<void> {
    this.triggers.delete(id);
    getDb().prepare('DELETE FROM triggers WHERE id = ?').run(id);
    log.info({ triggerId: id }, 'Trigger deleted');
  }

  /**
   * Get all triggers.
   */
  getTriggers(): Trigger[] {
    return Array.from(this.triggers.values());
  }

  /**
   * Handle contract event and fire matching triggers.
   */
  private async handleContractEvent(contractId: string, event: any, triggers: Trigger[]): Promise<void> {
    const e = {
      contractId,
      topics: (event.topic ?? []).map((t: any) => t.toString()),
      value: event.value?.toXDR?.('base64') ?? event.value ?? null,
      ledger: event.ledger,
    };

    for (const trigger of triggers) {
      if (!trigger.enabled || trigger.filter.contractId !== contractId) continue;
      if (!matchesFilter(e, trigger.filter)) continue;

      // Dedupe by event ID (contract event ID)
      const eventId = event.id;
      const isNew = await recordTriggerFired(trigger.id, eventId);
      if (!isNew) continue;

      log.info({ triggerId: trigger.id, eventId }, 'Contract event trigger firing');
      await this.agent.run(trigger.task);

      // Update last fired
      trigger.lastFiredAt = new Date().toISOString();
      await saveTrigger(trigger);
    }
  }

  /**
   * Handle storage change and fire matching triggers.
   */
  private async handleStorageChange(contractId: string, change: any): Promise<void> {
    const e = {
      contractId,
      storageKey: change.keyXdr,
      oldValue: change.oldValue,
      newValue: change.newValue,
      ledger: change.ledger,
    };

    for (const [id, trigger] of this.triggers) {
      if (!trigger.enabled || trigger.source !== 'storage_change') continue;
      if (trigger.filter.contractId !== contractId) continue;
      if (trigger.filter.storageKey && trigger.filter.storageKey !== change.keyXdr) continue;

      // Dedupe by ledger + key
      const eventId = `${e.ledger ?? 'unknown'}:${change.keyXdr}`;
      const isNew = await recordTriggerFired(id, eventId);
      if (!isNew) continue;

      log.info({ triggerId: id, eventId }, 'Storage change trigger firing');
      await this.agent.run(trigger.task);

      // Update last fired
      trigger.lastFiredAt = new Date().toISOString();
      await saveTrigger(trigger);
    }
  }

  /**
   * Stop all listeners and cleanup.
   */
  stop(): void {
    this.agent.stopListening();
    this.agent.stopContractListener();
    this.agent.stopStorageWatcher();
    
    // Stop contract event watchers
    if (this.unlistenContract) {
      this.unlistenContract();
    }
    
    // Stop storage watchers
    if (this.unlistenStorage) {
      this.unlistenStorage();
    }
    
    this.triggers.clear();
    this.contractWatchers.clear();
    this.storageWatchers.clear();
    log.info('TriggerManager stopped');
  }
}


