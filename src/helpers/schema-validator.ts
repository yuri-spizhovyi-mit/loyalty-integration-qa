import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import Ajv2020, { type ErrorObject, type ValidateFunction } from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { parse } from 'yaml';

/**
 * Validates payloads against the specs in `specs/`, which are the single
 * source of truth. Nothing here hard-codes a schema: response schemas are
 * looked up from the OpenAPI documents by operationId and status code, and
 * event schemas from the AsyncAPI documents by message name (= event type).
 */

/** The REST APIs with an OpenAPI document in specs/. */
export type ApiName = 'loyalty' | 'cdp' | 'notification';
/** The event publishers with an AsyncAPI document in specs/. */
export type EventSource = 'loyalty' | 'profile';

const OPENAPI_FILES: Record<ApiName, { file: string; id: string }> = {
  loyalty: { file: 'openapi.yaml', id: 'https://specs.loyalty.local/openapi.json' },
  cdp: { file: 'cdp-openapi.yaml', id: 'https://specs.loyalty.local/cdp-openapi.json' },
  notification: { file: 'notification-openapi.yaml', id: 'https://specs.loyalty.local/notification-openapi.json' },
};
const ASYNCAPI_FILES: Record<EventSource, { file: string; id: string }> = {
  loyalty: { file: 'asyncapi.yaml', id: 'https://specs.loyalty.local/asyncapi.json' },
  profile: { file: 'cdp-asyncapi.yaml', id: 'https://specs.loyalty.local/cdp-asyncapi.json' },
};
const HTTP_METHODS = ['get', 'post', 'put', 'patch', 'delete'] as const;

type Json = Record<string, unknown>;

export interface ValidationResult {
  valid: boolean;
  /** What the payload was checked against, e.g. "earnPoints 201". */
  against: string;
  errors: string[];
}

export interface OperationInfo {
  operationId: string;
  method: string;
  path: string;
  statuses: number[];
}

interface OpenApiDoc {
  paths: Record<string, Record<string, Json>>;
  components: { schemas: Record<string, Json>; responses: Record<string, Json> };
}

interface AsyncApiDoc {
  components: { schemas: Record<string, Json>; messages: Record<string, { name: string; payload: Json }> };
}

function specPath(file: string): string {
  return fileURLToPath(new URL(`../../specs/${file}`, import.meta.url));
}

/** Rewrites local refs such as `#/components/schemas/Tier` to `<id>#/$defs/Tier`. */
function rewriteRefs<T>(node: T, documentId: string): T {
  if (Array.isArray(node)) return node.map((item) => rewriteRefs(item, documentId)) as T;
  if (node === null || typeof node !== 'object') return node;
  const out: Json = {};
  for (const [key, value] of Object.entries(node as Json)) {
    out[key] =
      key === '$ref' && typeof value === 'string' && value.startsWith('#/components/schemas/')
        ? `${documentId}#/$defs/${value.slice('#/components/schemas/'.length)}`
        : rewriteRefs(value, documentId);
  }
  return out as T;
}

function formatErrors(errors: ErrorObject[] | null | undefined): string[] {
  return (errors ?? []).map((error) => {
    const where = error.instancePath || '(root)';
    const extra =
      error.keyword === 'additionalProperties'
        ? ` "${(error.params as { additionalProperty: string }).additionalProperty}"`
        : error.keyword === 'enum'
          ? ` ${JSON.stringify((error.params as { allowedValues: unknown[] }).allowedValues)}`
          : '';
    return `${where} ${error.message}${extra}`;
  });
}

export class ContractValidator {
  private readonly ajv: Ajv2020;
  private readonly compiled = new Map<string, ValidateFunction>();

  private constructor(
    readonly apis: Record<ApiName, OpenApiDoc>,
    readonly asyncapis: Record<EventSource, AsyncApiDoc>,
  ) {
    this.ajv = new Ajv2020({ strict: true, allErrors: true, strictTypes: false });
    addFormats(this.ajv);
    for (const name of Object.keys(OPENAPI_FILES) as ApiName[]) {
      const { id } = OPENAPI_FILES[name];
      this.ajv.addSchema({ $id: id, $defs: rewriteRefs(apis[name].components.schemas, id) });
    }
    for (const name of Object.keys(ASYNCAPI_FILES) as EventSource[]) {
      const { id } = ASYNCAPI_FILES[name];
      this.ajv.addSchema({ $id: id, $defs: rewriteRefs(asyncapis[name].components.schemas, id) });
    }
  }

  static load(): ContractValidator {
    const read = <T>(file: string) => parse(readFileSync(specPath(file), 'utf8')) as T;
    const load = <K extends string, T>(files: Record<K, { file: string }>) =>
      Object.fromEntries(Object.entries<{ file: string }>(files).map(([name, { file }]) => [name, read<T>(file)])) as Record<K, T>;
    return new ContractValidator(load<ApiName, OpenApiDoc>(OPENAPI_FILES), load<EventSource, AsyncApiDoc>(ASYNCAPI_FILES));
  }

  /** The loyalty engine's AsyncAPI document. */
  get asyncapi(): AsyncApiDoc {
    return this.asyncapis.loyalty;
  }

  /** The loyalty engine's OpenAPI document. */
  get openapi(): OpenApiDoc {
    return this.apis.loyalty;
  }

  /** Every operation in one OpenAPI document with its documented status codes. */
  operations(api: ApiName = 'loyalty'): OperationInfo[] {
    const result: OperationInfo[] = [];
    for (const [path, item] of Object.entries(this.apis[api].paths)) {
      for (const method of HTTP_METHODS) {
        const operation = item[method] as { operationId?: string; responses?: Json } | undefined;
        if (!operation) continue;
        result.push({
          operationId: operation.operationId ?? `(missing operationId: ${method.toUpperCase()} ${path})`,
          method: method.toUpperCase(),
          path,
          statuses: Object.keys(operation.responses ?? {}).map(Number),
        });
      }
    }
    return result;
  }

  /** Event types (AsyncAPI message names) one publisher may send. */
  eventTypes(source: EventSource = 'loyalty'): string[] {
    return Object.values(this.asyncapis[source].components.messages).map((message) => message.name);
  }

  validateResponse(operationId: string, status: number, body: unknown): ValidationResult {
    const against = `${operationId} ${status}`;
    const found = this.findOperation(operationId);
    if (!found) return { valid: false, against, errors: [`operation ${operationId} is not in any OpenAPI document`] };
    const { operation, api } = found;

    const responses = (operation.responses ?? {}) as Record<string, Json>;
    let response = responses[String(status)];
    if (!response) {
      return {
        valid: false,
        against,
        errors: [`status ${status} is not documented (documented: ${Object.keys(responses).join(', ')})`],
      };
    }
    if (typeof response.$ref === 'string') {
      response = this.apis[api].components.responses[response.$ref.split('/').pop() as string] as Json;
    }
    if (response.content === undefined) {
      // A documented no-content response, e.g. 204.
      return body === undefined
        ? { valid: true, against, errors: [] }
        : { valid: false, against, errors: ['response is documented without a body, but has one'] };
    }
    const schema = (response.content as Record<string, { schema: Json }> | undefined)?.['application/json']?.schema;
    if (!schema) return { valid: false, against, errors: ['response has no application/json schema'] };
    return this.run(`response:${against}`, schema, OPENAPI_FILES[api].id, body, against);
  }

  validateRequest(operationId: string, body: unknown): ValidationResult {
    const against = `${operationId} request`;
    const found = this.findOperation(operationId);
    const requestBody = found?.operation.requestBody as { content?: Record<string, { schema: Json }> } | undefined;
    const schema = requestBody?.content?.['application/json']?.schema;
    if (!schema) return { valid: false, against, errors: [`${operationId} has no JSON request body schema`] };
    return this.run(`request:${operationId}`, schema, OPENAPI_FILES[found!.api].id, body, against);
  }

  /** Validates against a named OpenAPI component, e.g. "Error". */
  validateComponent(name: string, body: unknown, api: ApiName = 'loyalty'): ValidationResult {
    const ref = { $ref: `#/components/schemas/${name}` };
    return this.run(`component:${api}:${name}`, ref, OPENAPI_FILES[api].id, body, name);
  }

  /**
   * Picks the AsyncAPI message from the event's own `type` (message names are
   * unique across documents; a spec test checks it) and validates the event.
   */
  validateEvent(event: unknown): ValidationResult {
    const type = (event as { type?: unknown } | null)?.type;
    const against = `event ${String(type)}`;
    for (const source of Object.keys(ASYNCAPI_FILES) as EventSource[]) {
      const message = Object.values(this.asyncapis[source].components.messages).find((m) => m.name === type);
      if (message) return this.run(`event:${message.name}`, message.payload, ASYNCAPI_FILES[source].id, event, against);
    }
    return { valid: false, against, errors: [`type ${JSON.stringify(type)} is not a message in any AsyncAPI document`] };
  }

  /** operationIds are unique across all OpenAPI documents (a spec test checks it). */
  private findOperation(operationId: string): { operation: Json; api: ApiName } | undefined {
    for (const api of Object.keys(this.apis) as ApiName[]) {
      for (const item of Object.values(this.apis[api].paths)) {
        for (const method of HTTP_METHODS) {
          const operation = item[method];
          if (operation && operation.operationId === operationId) return { operation, api };
        }
      }
    }
    return undefined;
  }

  private run(cacheKey: string, schema: Json, documentId: string, body: unknown, against: string): ValidationResult {
    let validate = this.compiled.get(cacheKey);
    if (!validate) {
      validate = this.ajv.compile(rewriteRefs(schema, documentId));
      this.compiled.set(cacheKey, validate);
    }
    const valid = validate(body) as boolean;
    return { valid, against, errors: valid ? [] : formatErrors(validate.errors) };
  }
}

let shared: ContractValidator | undefined;

/** One validator per worker process; compiling schemas is the slow part. */
export function getContract(): ContractValidator {
  shared ??= ContractValidator.load();
  return shared;
}
