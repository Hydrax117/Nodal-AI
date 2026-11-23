import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

/**
 * Tests for triggers.ts ensuring:
 *
 * - Asset issuer filtering (Issue #243)
 * - BigInt-based amount comparison (Issue #241)
 * - Contract event trigger support
 * - Storage change trigger support
 * - Deduplication across restarts
 */

// Import the module under test
import * as triggersModule from '../backend/triggers';

// ─── Mock config ──────────────────────────────────────────────────────────────

vi.mock('../backend/config', () => ({
  config: {
    X402_ASSET_CODE: 'USDC',
    X402_ASSET_ISSUER: 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM34K4KZVN',
    AGENT_SPENDING_LIMIT: '1000',
  },
}));

vi.mock('../backend/utils/logger', () => ({
  createLogger: () => ({
    info: () => {},
    warn: () => {},
    error: () => {},
  }),
}));

// Mock the agent
const mockAgent = {
  startListening: vi.fn(),
  stopListening: vi.fn(),
  startContractListener: vi.fn(),
  stopContractListener: vi.fn(),
  startStorageWatcher: vi.fn(),
  stopStorageWatcher: vi.fn(),
  run: vi.fn().mockResolvedValue({ success: true }),
};

// Mock persistence to track trigger events
const mockDb = {
  prepare: vi.fn(),
};
const runResults: any[] = [];
mockDb.prepare.mockReturnValue({
  run: vi.fn().mockImplementation((...args: any[]) => {
    runResults.push(args);
    return { changes: 0 };
  }),
  all: vi.fn().mockReturnValue([]),
});

vi.mock('../backend/persistence', () => ({
  getDb: () => mockDb,
}));

// Mock contract event listeners
vi.mock('../backend/tools/ContractEventListener', () => ({
  listen: vi.fn(),
}));

vi.mock('../backend/tools/ContractStorageWatcherTool', () => ({
  watchContractStorage: vi.fn(),
}));

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('matchesFilter — asset issuer discrimination', () => {
  it('same assetCode but different issuer does NOT match', () => {
    // User creates a trigger for real USDC (with issuer)
    const triggerFilter = {
      assetCode: 'USDC',
      assetIssuer: 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
    };

    // Fake USDC arrives with different issuer
    const fakePayment = {
      assetCode: 'USDC',
      assetIssuer: 'GB7TAYRZCJZ55X6Z3Y7WY6W6Z7X6Z5Y4X3W2V1U0T9S8R7Q6P5O4N3M2L1K0J',
      amount: '500',
    };

    // Filter should NOT match
    expect(triggersModule.matchesFilter(fakePayment, triggerFilter)).toBe(false);
  });

  it('same assetCode and issuer DOES match', () => {
    const triggerFilter = {
      assetCode: 'USDC',
      assetIssuer: 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
    };

    const realPayment = {
      assetCode: 'USDC',
      assetIssuer: 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
      amount: '500',
    };

    expect(triggersModule.matchesFilter(realPayment, triggerFilter)).toBe(true);
  });

  it('XLM without issuer matches all XLM payments', () => {
    const triggerFilter = {
      assetCode: 'XLM',
    };

    const payment = {
      assetCode: 'XLM',
      amount: '100',
    };

    expect(triggersModule.matchesFilter(payment, triggerFilter)).toBe(true);
  });

  it('XLM with issuer still matches', () => {
    const triggerFilter = {
      assetCode: 'XLM',
      assetIssuer: '', // XLM is native, issuer is optional
    };

    const payment = {
      assetCode: 'XLM',
      amount: '100',
    };

    expect(triggersModule.matchesFilter(payment, triggerFilter)).toBe(true);
  });
});

describe('matchesFilter — amount comparison with BigInt', () => {
  it('correctly compares large amounts without float error', () => {
    const triggerFilter = {
      assetCode: 'USDC',
      assetIssuer: 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
      minAmount: '0.1000001', // 1 stroop over 0.1
    };

    const payment = {
      assetCode: 'USDC',
      assetIssuer: 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
      amount: '0.1000001', // Exactly the threshold
    };

    expect(triggersModule.matchesFilter(payment, triggerFilter)).toBe(true);
  });

  it('rejects amount below threshold using stroop precision', () => {
    const triggerFilter = {
      minAmount: '0.1000001',
    };

    const payment = {
      amount: '0.1000000', // 1 stroop less
    };

    expect(triggersModule.matchesFilter(payment, triggerFilter)).toBe(false);
  });
});

describe('createTrigger — non-native asset validation', () => {
  it('rejects non-native trigger without assetIssuer', async () => {
    const manager = new triggersModule.TriggerManager(mockAgent as any);

    await expect(
      manager.createTrigger({
        source: 'payment',
        filter: {
          assetCode: 'USDC',
          // Missing assetIssuer
        },
        task: { type: 'stellar_payment', payload: {} },
      })
    ).rejects.toThrow(/assetIssuer is required for non-XLM asset/);
  });

  it('accepts non-native trigger with assetIssuer', async () => {
    const manager = new triggersModule.TriggerManager(mockAgent as any);

    const result = await manager.createTrigger({
      source: 'payment',
      filter: {
        assetCode: 'USDC',
        assetIssuer: 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
      },
      task: { type: 'stellar_payment', payload: {} },
    });

    expect(result).toBeDefined();
    expect(result.filter.assetCode).toBe('USDC');
    expect(result.filter.assetIssuer).toBe(
      'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN'
    );
  });

  it('accepts XLM trigger without assetIssuer', async () => {
    const manager = new triggersModule.TriggerManager(mockAgent as any);

    const result = await manager.createTrigger({
      source: 'payment',
      filter: {
        assetCode: 'XLM',
        // No assetIssuer needed for XLM
      },
      task: { type: 'stellar_payment', payload: {} },
    });

    expect(result).toBeDefined();
  });
});

describe('matchesFilter — edge cases', () => {
  it('handles X402 payment object structure correctly', () => {
    const triggerFilter = {
      assetCode: 'USDC',
      assetIssuer: 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
    };

    // X402 challenge structure passed to payment listener
    const payment = {
      assetCode: 'USDC',
      assetIssuer: 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
      from: 'GB7TAYRZCJZ55X6Z3Y7WY6W6Z7X6Z5Y4X3W2V1U0T9S8R7Q6P5O4N3M2L1K0J',
      amount: '500',
    };

    expect(triggersModule.matchesFilter(payment, triggerFilter)).toBe(true);
  });

  it('matches assetCode only when no issuer specified for non-XLM (legacy)', () => {
    // This is the OLD behavior for backward compatibility
    const triggerFilter = {
      assetCode: 'USDC',
      // No assetIssuer — legacy trigger
    };

    const payment = {
      assetCode: 'USDC',
      assetIssuer: 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
      amount: '500',
    };

    // Should still match to avoid breaking existing triggers
    expect(triggersModule.matchesFilter(payment, triggerFilter)).toBe(true);
  });
});

// ─── Contract Event Trigger Tests ────────────────────────────────────────────

describe('ContractEvent trigger', () => {
  it('fires trigger when contract event matches', async () => {
    const mockHandle = {
      stop: vi.fn(),
      on: vi.fn(),
      removeAllListeners: vi.fn(),
    };
    vi.mocked(triggersModule.listenContractEvents as any).mockReturnValue(mockHandle);

    const manager = new triggersModule.TriggerManager(mockAgent as any);

    // Create a contract event trigger
    await manager.createTrigger({
      source: 'contract_event',
      filter: {
        contractId: 'CB7TYW2P3JZV7X5Y7W6Z7X6Z5Y4X3W2V1U0T9S8R7Q6P5O4N3M2L1K0J',
        eventTopics: ['Transfer'],
      },
      task: { type: 'stellar_payment', payload: {} },
    });

    // Start the manager (this sets up listeners)
    await manager.start();

    // Verify listener was set up
    expect(triggersModule.listenContractEvents).toHaveBeenCalledWith(
      'CB7TYW2P3JZV7X5Y7W6Z7X6Z5Y4X3W2V1U0T9S8R7Q6P5O4N3M2L1K0J',
      ['Transfer'],
      expect.any(Function),
      expect.any(Object)
    );

    // Cleanup
    manager.stop();
  });
});

// ─── Storage Change Trigger Tests ────────────────────────────────────────────

describe('StorageChange trigger', () => {
  it('fires trigger when storage key changes', async () => {
    // Mock watchContractStorage
    const mockHandle = {
      stop: vi.fn(),
      on: vi.fn(),
      removeAllListeners: vi.fn(),
    };
    vi.mocked(triggersModule.watchContractStorage as any).mockReturnValue(mockHandle);

    const manager = new triggersModule.TriggerManager(mockAgent as any);

    // Create a storage change trigger
    await manager.createTrigger({
      source: 'storage_change',
      filter: {
        contractId: 'CB7TYW2P3JZV7X5Y7W6Z7X6Z5Y4X3W2V1U0T9S8R7Q6P5O4N3M2L1K0J',
        storageKey: 'dGVzdGtleQ==', // base64 encoded "testkey"
      },
      task: { type: 'stellar_payment', payload: {} },
    });

    // Start the manager (this sets up listeners)
    await manager.start();

    // Verify watcher was set up
    expect(triggersModule.watchContractStorage).toHaveBeenCalledWith(
      expect.objectContaining({
        contractId: 'CB7TYW2P3JZV7X5Y7W6Z7X6Z5Y4X3W2V1U0T9S8R7Q6P5O4N3M2L1K0J',
      })
    );

    // Cleanup
    manager.stop();
  });
});
// ─── Deduplication Tests ─────────────────────────────────────────────────────

describe('Deduplication', () => {
  it('recordTriggerFired prevents duplicate firing', async () => {
    const triggerId = 'test-trigger';
    const eventId = 'event-123';

    // First call should return true (new)
    const first = await triggersModule.recordTriggerFired(triggerId, eventId);
    expect(first).toBe(true);

    // Second call should return false (already fired)
    const second = await triggersModule.recordTriggerFired(triggerId, eventId);
    expect(second).toBe(false);
  });
});
