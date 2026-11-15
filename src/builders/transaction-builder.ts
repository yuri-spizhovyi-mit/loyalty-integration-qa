import { randomUUID } from 'node:crypto';
import type { PointsBody, RefundBody } from '../clients/loyalty-types.js';

/** Body for earn or redeem. Defaults to 100 points with a unique order reference. */
export class PointsBuilder {
  private body: PointsBody = { points: 100, reference: `order-${randomUUID().slice(0, 8)}` };

  points(points: number): this {
    this.body.points = points;
    return this;
  }

  reference(reference: string): this {
    this.body.reference = reference;
    return this;
  }

  withoutReference(): this {
    delete this.body.reference;
    return this;
  }

  build(): PointsBody {
    return { ...this.body };
  }
}

export class RefundBuilder {
  private body: RefundBody;

  constructor(transactionId: string) {
    this.body = { transactionId, reference: `return-${randomUUID().slice(0, 8)}` };
  }

  points(points: number): this {
    this.body.points = points;
    return this;
  }

  build(): RefundBody {
    return { ...this.body };
  }
}

export const anEarn = () => new PointsBuilder();
export const aRedeem = () => new PointsBuilder();
export const aRefundOf = (transactionId: string) => new RefundBuilder(transactionId);

export function uniqueIdempotencyKey(): string {
  return `idem-${randomUUID()}`;
}
