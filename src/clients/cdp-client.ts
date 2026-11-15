import type { APIRequestContext, APIResponse } from '@playwright/test';
import { checkedResponse } from '../helpers/contract-check.js';
import type { ContractValidator } from '../helpers/schema-validator.js';
import type { FaultMode, Profile } from './cdp-types.js';

/**
 * Service object for the customer data platform. Every response is checked
 * against cdp-openapi.yaml when a ContractValidator is given.
 */
export class CdpClient {
  constructor(
    private readonly request: APIRequestContext,
    private readonly contract?: ContractValidator,
  ) {}

  health(): Promise<APIResponse> {
    return this.checked('getCdpHealth', this.request.get('/health'));
  }

  getProfile(profileId: string): Promise<APIResponse> {
    return this.checked('getProfile', this.request.get(`/profiles/${encodeURIComponent(profileId)}`));
  }

  /** The profile, or null while no event for this customer has been applied. For polling. */
  async profileOrNull(profileId: string): Promise<Profile | null> {
    const response = await this.getProfile(profileId);
    if (response.status() === 404) return null;
    if (response.status() !== 200) throw new Error(`GET profile ${profileId}: unexpected status ${response.status()}`);
    return (await response.json()) as Profile;
  }

  /** Delivers an event straight to the consumer, bypassing the broker. */
  sendEvent(rawBody: string): Promise<APIResponse> {
    return this.checked(
      'receiveEvent',
      // A Buffer so the bytes go out exactly as given (see LoyaltyClient.postRaw).
      this.request.post('/events', { data: Buffer.from(rawBody), headers: { 'Content-Type': 'application/json' } }),
    );
  }

  /** Test only: fail the next `times` deliveries for this customer. */
  setFault(customerId: string, mode: FaultMode | string, times: number): Promise<APIResponse> {
    return this.checked(
      'setFault',
      this.request.put(`/internal/faults/${encodeURIComponent(customerId)}`, { data: { mode, times } }),
    );
  }

  clearFault(customerId: string): Promise<APIResponse> {
    return this.checked('clearFault', this.request.delete(`/internal/faults/${encodeURIComponent(customerId)}`));
  }

  private checked(operationId: string, pending: Promise<APIResponse>): Promise<APIResponse> {
    return checkedResponse(this.contract, operationId, pending);
  }
}
