import { aCustomer } from '../../src/builders/customer-builder.js';
import { aRedeem, aRefundOf } from '../../src/builders/transaction-builder.js';
import type { CdpClient } from '../../src/clients/cdp-client.js';
import type { Profile } from '../../src/clients/cdp-types.js';
import type { LoyaltyClient } from '../../src/clients/loyalty-client.js';
import type {
  CreateCustomerBody,
  LoyaltyEvent,
  OutboxResponse,
  TransactionResponse,
} from '../../src/clients/loyalty-types.js';
import { expect, test } from '../../src/fixtures.js';
import { readJson } from '../../src/helpers/json.js';
import { getContract } from '../../src/helpers/schema-validator.js';
import { waitUntilInSync } from '../../src/helpers/sync.js';

/**
 * Field mapping between the loyalty engine and the customer data platform,
 * one test per row of the mapping table in cdp-openapi.yaml. The rows are
 * read from the spec, and a coverage test fails if a row has no check here,
 * so the table and the tests cannot drift apart.
 *
 * Each check runs end to end (REST -> event -> broker -> profile) and takes
 * the expected value from the event the engine actually published.
 */

interface Ctx {
  loyalty: LoyaltyClient;
  cdp: CdpClient;
  createCustomer: (body?: CreateCustomerBody) => Promise<CreateCustomerBody>;
  earnPoints: (customerId: string, points: number) => Promise<TransactionResponse>;
}

/** Parses the markdown table in the spec description into "event | field | attribute" keys. */
function mappingRows(): string[] {
  const description = (getContract().apis.cdp as unknown as { info: { description: string } }).info.description;
  return description
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.startsWith('|') && !line.startsWith('|---') && !line.startsWith('| Event'))
    .map((line) =>
      line
        .slice(1, -1)
        .split('|')
        .map((cell) => cell.trim())
        .join(' | '),
    );
}

async function synced(ctx: Ctx, customerId: string) {
  const profile = await waitUntilInSync(ctx.loyalty, ctx.cdp, customerId);
  const { events } = await readJson<OutboxResponse>(await ctx.loyalty.getOutbox(customerId));
  const last = (type: LoyaltyEvent['type']) => events.filter((event) => event.type === type).at(-1)!;
  return { profile, events, last };
}

type Check = (ctx: Ctx) => Promise<void>;

const checks: Record<string, Check> = {
  'customer.created | customerId | profileId': async (ctx) => {
    const { customerId } = await ctx.createCustomer();
    const { profile, last } = await synced(ctx, customerId);
    expect(profile.profileId).toBe(last('customer.created').customerId);
  },

  'customer.created | data.tier ("Bronze") | attributes.loyalty_tier ("bronze")': async (ctx) => {
    const { customerId } = await ctx.createCustomer();
    const { profile, last } = await synced(ctx, customerId);
    expect(last('customer.created').data.tier).toBe('Bronze');
    expect(profile.attributes.loyalty_tier).toBe('bronze');
  },

  'customer.created | data.marketingConsent | attributes.marketing_consent': async (ctx) => {
    for (const consent of [true, false]) {
      const { customerId } = await ctx.createCustomer(aCustomer().withMarketingConsent(consent).build());
      const { profile, last } = await synced(ctx, customerId);
      expect(last('customer.created').data.marketingConsent, `consent ${consent}`).toBe(consent);
      expect(profile.attributes.marketing_consent, `consent ${consent}`).toBe(consent);
    }
  },

  'customer.created | occurredAt | attributes.member_since': async (ctx) => {
    const { customerId } = await ctx.createCustomer();
    await ctx.earnPoints(customerId, 10); // later activity must not move member_since
    const { profile, last } = await synced(ctx, customerId);
    expect(profile.attributes.member_since).toBe(last('customer.created').occurredAt);
  },

  'customer.created | (new member) | attributes.points_balance = 0': async (ctx) => {
    const { customerId } = await ctx.createCustomer();
    const { profile } = await synced(ctx, customerId);
    expect(profile.attributes.points_balance).toBe(0);
  },

  'tier.changed | data.newTier ("Gold") | attributes.loyalty_tier ("gold")': async (ctx) => {
    const { customerId } = await ctx.createCustomer();
    let goldEarn = '';
    const steps: [string, () => Promise<unknown>][] = [
      ['silver', () => ctx.earnPoints(customerId, 1000)],
      ['gold', async () => (goldEarn = (await ctx.earnPoints(customerId, 4000)).transaction.transactionId)],
      // A downgrade maps the same way: refunding the 4000 leaves 1000 qualifying points.
      ['silver', async () => expect(await ctx.loyalty.refund(customerId, aRefundOf(goldEarn).build())).toHaveStatus(201)],
    ];
    for (const [tier, write] of steps) {
      await write();
      const { profile, last } = await synced(ctx, customerId);
      const event = last('tier.changed');
      expect(String(event.data.newTier).toLowerCase(), `tier.changed ${event.sequence}`).toBe(tier);
      expect(profile.attributes.loyalty_tier, `after tier.changed ${event.sequence}`).toBe(tier);
    }
  },

  'points.* | data.balanceAfter | attributes.points_balance': async (ctx) => {
    const { customerId } = await ctx.createCustomer();
    const earn = await ctx.earnPoints(customerId, 700);
    for (const [label, write] of [
      ['earn', async () => {}],
      ['redeem', async () => expect(await ctx.loyalty.redeem(customerId, aRedeem().points(150).build())).toHaveStatus(201)],
      ['refund', async () => expect(await ctx.loyalty.refund(customerId, aRefundOf(earn.transaction.transactionId).points(50).build())).toHaveStatus(201)],
    ] as const) {
      await write();
      const { profile, events } = await synced(ctx, customerId);
      const lastTransaction = events.filter((e) => e.type.startsWith('points.')).at(-1)!;
      expect(profile.attributes.points_balance, label).toBe(lastTransaction.data.balanceAfter);
    }
  },

  'points.* | data.transactionId | attributes.last_transaction_id': async (ctx) => {
    const { customerId } = await ctx.createCustomer();
    const first = await ctx.earnPoints(customerId, 300);
    const second = await ctx.loyalty.redeem(customerId, aRedeem().points(100).build());
    await expect(second).toHaveStatus(201);
    const { profile, last } = await synced(ctx, customerId);
    expect(profile.attributes.last_transaction_id).toBe(last('points.redeemed').data.transactionId);
    expect(profile.attributes.last_transaction_id).not.toBe(first.transaction.transactionId);
  },

  'points.* | data.entryType ("EARN") | attributes.last_transaction_type ("earn")': async (ctx) => {
    const { customerId } = await ctx.createCustomer();
    const earn = await ctx.earnPoints(customerId, 500);
    const writes: [string, () => Promise<unknown>][] = [
      ['earn', async () => {}],
      ['redeem', async () => expect(await ctx.loyalty.redeem(customerId, aRedeem().points(100).build())).toHaveStatus(201)],
      ['refund', async () => expect(await ctx.loyalty.refund(customerId, aRefundOf(earn.transaction.transactionId).points(100).build())).toHaveStatus(201)],
    ];
    for (const [expected, write] of writes) {
      await write();
      const { profile, events } = await synced(ctx, customerId);
      const lastTransaction = events.filter((e) => e.type.startsWith('points.')).at(-1)!;
      expect(String(lastTransaction.data.entryType).toLowerCase()).toBe(expected);
      expect(profile.attributes.last_transaction_type).toBe(expected);
    }
  },

  'any | occurredAt | attributes.last_activity_at': async (ctx) => {
    const { customerId } = await ctx.createCustomer();
    await ctx.earnPoints(customerId, 1000); // points.earned and tier.changed
    const { profile, events } = await synced(ctx, customerId);
    const latest = events.reduce((a, b) => (b.sequence > a.sequence ? b : a));
    expect(latest.type).toBe('tier.changed');
    expect(profile.attributes.last_activity_at).toBe(latest.occurredAt);
  },
};

test.describe('Field mapping: engine events -> customer profile (cdp-openapi.yaml)', () => {
  test('every row of the mapping table has a check, and every check has a row', () => {
    expect(Object.keys(checks).sort()).toEqual(mappingRows().sort());
  });

  for (const row of mappingRows()) {
    test(row, async ({ loyalty, cdp, createCustomer, earnPoints }) => {
      const check = checks[row];
      test.skip(!check, 'no check for this row; the coverage test above fails');
      await check!({ loyalty, cdp, createCustomer, earnPoints });
    });
  }

  test('every profile attribute is covered by a mapping row', () => {
    const profile = getContract().apis.cdp.components.schemas.Profile as {
      properties: { attributes: { required: string[] } };
    };
    const mapped = new Set(mappingRows().flatMap((row) => row.match(/attributes\.(\w+)/g) ?? []));
    for (const attribute of profile.properties.attributes.required) {
      expect(mapped, `attributes.${attribute} has no row in the mapping table`).toContain(`attributes.${attribute}`);
    }
  });
});

test.describe('Cross-system reconciliation', () => {
  test('after a mixed history, the profile agrees with the engine on every mapped field', async ({
    loyalty,
    cdp,
    createCustomer,
    earnPoints,
  }) => {
    const { customerId } = await createCustomer(aCustomer().withMarketingConsent(true).build());
    const earn = await earnPoints(customerId, 5200);
    await expect(await loyalty.redeem(customerId, aRedeem().points(2000).build())).toHaveStatus(201);
    await expect(await loyalty.refund(customerId, aRefundOf(earn.transaction.transactionId).points(300).build())).toHaveStatus(201);
    await expect(await loyalty.redeem(customerId, aRedeem().points(99_999).build())).toHaveStatus(422);

    const profile: Profile = await waitUntilInSync(loyalty, cdp, customerId);
    const engine = await loyalty.getCustomer(customerId).then((r) => r.json());
    const ledger = await loyalty.getLedger(customerId).then((r) => r.json());
    const lastEntry = ledger.entries.at(-1);

    expect(profile.attributes).toEqual({
      loyalty_tier: engine.tier.toLowerCase(),
      points_balance: engine.balance,
      marketing_consent: engine.marketingConsent,
      member_since: expect.any(String),
      last_transaction_id: lastEntry.transactionId,
      last_transaction_type: lastEntry.type.toLowerCase(),
      last_activity_at: expect.any(String),
    });
    expect(engine).toMatchObject({ tier: 'Silver', balance: 2900 });
  });
});
