import { type APIResponse, expect as baseExpect } from '@playwright/test';
import { getContract, type ValidationResult } from './schema-validator.js';

const MAX_BODY_CHARS = 2000;

/** Everything needed to find a failing request in logs: URL, correlation ID and body. */
export async function describeResponse(response: APIResponse): Promise<string> {
  let body: string;
  try {
    body = await response.text();
  } catch {
    body = '(body unavailable)';
  }
  if (body.length > MAX_BODY_CHARS) body = `${body.slice(0, MAX_BODY_CHARS)}... (truncated)`;
  return [
    `  url:           ${response.url()}`,
    `  correlationId: ${response.headers()['x-correlation-id'] ?? '(none)'}`,
    `  body:          ${body}`,
  ].join('\n');
}

interface ErrorBody {
  error?: { code?: string };
}

function schemaMessage(result: ValidationResult, isNot: boolean, payload: unknown, extra = ''): string {
  const header = isNot
    ? `Expected payload NOT to match ${result.against}, but it does`
    : `Payload does not match ${result.against}:`;
  const errors = result.errors.map((error) => `  - ${error}`).join('\n');
  const shown = JSON.stringify(payload);
  return `${header}\n${errors}\n${extra}  payload: ${shown.length > MAX_BODY_CHARS ? `${shown.slice(0, MAX_BODY_CHARS)}...` : shown}`;
}

export const expect = baseExpect.extend({
  /** `await expect(response).toHaveStatus(201)`; on failure prints URL, correlation ID and body. */
  async toHaveStatus(response: APIResponse, expected: number) {
    const actual = response.status();
    const pass = actual === expected;
    const details = await describeResponse(response);
    return {
      pass,
      name: 'toHaveStatus',
      expected,
      actual,
      message: () =>
        `${this.utils.matcherHint('toHaveStatus', undefined, undefined, { isNot: this.isNot })}\n\n` +
        `Expected status: ${this.isNot ? 'not ' : ''}${expected}\n` +
        `Received status: ${actual}\n${details}`,
    };
  },

  /** `await expect(response).toBeApiError(422, 'INSUFFICIENT_BALANCE')`: status and error code together. */
  async toBeApiError(response: APIResponse, status: number, code: string) {
    const actualStatus = response.status();
    let actualCode: string | undefined;
    try {
      actualCode = ((await response.json()) as ErrorBody).error?.code;
    } catch {
      actualCode = undefined;
    }
    const pass = actualStatus === status && actualCode === code;
    const details = await describeResponse(response);
    return {
      pass,
      name: 'toBeApiError',
      expected: `${status} ${code}`,
      actual: `${actualStatus} ${actualCode ?? '(no error code)'}`,
      message: () =>
        `${this.utils.matcherHint('toBeApiError', undefined, undefined, { isNot: this.isNot })}\n\n` +
        `Expected: ${this.isNot ? 'not ' : ''}${status} ${code}\n` +
        `Received: ${actualStatus} ${actualCode ?? '(no error code)'}\n${details}`,
    };
  },

  /** `await expect(response).toMatchContract('earnPoints')`: body vs the OpenAPI schema for its status. */
  async toMatchContract(response: APIResponse, operationId: string) {
    let body: unknown;
    try {
      body = await response.json();
    } catch {
      body = '(not JSON)';
    }
    const result = getContract().validateResponse(operationId, response.status(), body);
    const context =
      `  url:           ${response.url()}\n` +
      `  correlationId: ${response.headers()['x-correlation-id'] ?? '(none)'}\n`;
    return {
      pass: result.valid,
      name: 'toMatchContract',
      message: () => schemaMessage(result, this.isNot, body, context),
    };
  },

  /** `expect(event).toMatchEventSchema()`: the AsyncAPI message is chosen by the event's `type`. */
  toMatchEventSchema(event: unknown) {
    const result = getContract().validateEvent(event);
    return {
      pass: result.valid,
      name: 'toMatchEventSchema',
      message: () => schemaMessage(result, this.isNot, event),
    };
  },

  /** `expect(body).toMatchSchema('Error')`: validates against a named OpenAPI component. */
  toMatchSchema(payload: unknown, componentName: string) {
    const result = getContract().validateComponent(componentName, payload);
    return {
      pass: result.valid,
      name: 'toMatchSchema',
      message: () => schemaMessage(result, this.isNot, payload),
    };
  },
});
