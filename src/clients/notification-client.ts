import type { APIRequestContext, APIResponse } from '@playwright/test';
import { checkedResponse } from '../helpers/contract-check.js';
import type { ContractValidator } from '../helpers/schema-validator.js';
import type { FaultMode } from './cdp-types.js';
import type { Decision, Notification } from './notification-types.js';

/**
 * Service object for the notification service. Every response is checked
 * against notification-openapi.yaml when a ContractValidator is given.
 */
export class NotificationClient {
  constructor(
    private readonly request: APIRequestContext,
    private readonly contract?: ContractValidator,
  ) {}

  health(): Promise<APIResponse> {
    return this.checked('getNotificationHealth', this.request.get('/health'));
  }

  listNotifications(customerId: string): Promise<APIResponse> {
    return this.checked(
      'listNotifications',
      this.request.get(`/customers/${encodeURIComponent(customerId)}/notifications`),
    );
  }

  listDecisions(customerId: string): Promise<APIResponse> {
    return this.checked('listDecisions', this.request.get(`/customers/${encodeURIComponent(customerId)}/decisions`));
  }

  async notifications(customerId: string): Promise<Notification[]> {
    const response = await this.listNotifications(customerId);
    if (response.status() !== 200) throw new Error(`notifications for ${customerId}: ${response.status()}`);
    return ((await response.json()) as { notifications: Notification[] }).notifications;
  }

  async decisions(customerId: string): Promise<Decision[]> {
    const response = await this.listDecisions(customerId);
    if (response.status() !== 200) throw new Error(`decisions for ${customerId}: ${response.status()}`);
    return ((await response.json()) as { decisions: Decision[] }).decisions;
  }

  /** Delivers an event straight to the consumer, bypassing the broker. */
  sendEvent(rawBody: string): Promise<APIResponse> {
    return this.checked(
      'receiveProfileEvent',
      this.request.post('/events', { data: Buffer.from(rawBody), headers: { 'Content-Type': 'application/json' } }),
    );
  }

  /** Test only: fail the next `times` deliveries for this customer. */
  setFault(customerId: string, mode: FaultMode | string, times: number): Promise<APIResponse> {
    return this.checked(
      'setNotificationFault',
      this.request.put(`/internal/faults/${encodeURIComponent(customerId)}`, { data: { mode, times } }),
    );
  }

  clearFault(customerId: string): Promise<APIResponse> {
    return this.checked(
      'clearNotificationFault',
      this.request.delete(`/internal/faults/${encodeURIComponent(customerId)}`),
    );
  }

  private checked(operationId: string, pending: Promise<APIResponse>): Promise<APIResponse> {
    return checkedResponse(this.contract, operationId, pending);
  }
}
