import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import Ajv2020, { type ErrorObject, type ValidateFunction } from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { parse } from 'yaml';

/**
 * Validates payloads against the specs in `specs/`, which are the single
 * source of truth. Nothing here hard-codes a schema: response schemas are
 * looked up from the OpenAPI document by operationId and status code, and
 * event schemas from the AsyncAPI document by message name (= event type).
 */

const OPENAPI_ID = 'https://specs.loyalty.local/openapi.json';
const ASYNCAPI_ID = 'https://specs.loyalty.local/asyncapi.json';
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
    readonly openapi: OpenApiDoc,
    readonly asyncapi: AsyncApiDoc,
  ) {
    this.ajv = new Ajv2020({ strict: true, allErrors: true, strictTypes: false });
    addFormats(this.ajv);
    this.ajv.addSchema({ $id: OPENAPI_ID, $defs: rewriteRefs(openapi.components.schemas, OPENAPI_ID) });
    this.ajv.addSchema({ $id: ASYNCAPI_ID, $defs: rewriteRefs(asyncapi.components.schemas, ASYNCAPI_ID) });
  }

  static load(): ContractValidator {
    const openapi = parse(readFileSync(specPath('openapi.yaml'), 'utf8')) as OpenApiDoc;
    const asyncapi = parse(readFileSync(specPath('asyncapi.yaml'), 'utf8')) as AsyncApiDoc;
    return new ContractValidator(openapi, asyncapi);
  }

  /** Every operation in the OpenAPI document with its documented status codes. */
  operations(): OperationInfo[] {
    const result: OperationInfo[] = [];
    for (const [path, item] of Object.entries(this.openapi.paths)) {
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

  /** Event types (AsyncAPI message names) the engine may publish. */
  eventTypes(): string[] {
    return Object.values(this.asyncapi.components.messages).map((message) => message.name);
  }

  validateResponse(operationId: string, status: number, body: unknown): ValidationResult {
    const against = `${operationId} ${status}`;
    const operation = this.findOperation(operationId);
    if (!operation) return { valid: false, against, errors: [`operation ${operationId} is not in openapi.yaml`] };

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
      response = this.openapi.components.responses[response.$ref.split('/').pop() as string] as Json;
    }
    const schema = (response.content as Record<string, { schema: Json }> | undefined)?.['application/json']?.schema;
    if (!schema) return { valid: false, against, errors: ['response has no application/json schema'] };
    return this.run(`response:${against}`, schema, OPENAPI_ID, body, against);
  }

  validateRequest(operationId: string, body: unknown): ValidationResult {
    const against = `${operationId} request`;
    const operation = this.findOperation(operationId);
    const requestBody = operation?.requestBody as { content?: Record<string, { schema: Json }> } | undefined;
    const schema = requestBody?.content?.['application/json']?.schema;
    if (!schema) return { valid: false, against, errors: [`${operationId} has no JSON request body schema`] };
    return this.run(`request:${operationId}`, schema, OPENAPI_ID, body, against);
  }

  /** Validates against a named OpenAPI component, e.g. "Error". */
  validateComponent(name: string, body: unknown): ValidationResult {
    return this.run(`component:${name}`, { $ref: `#/components/schemas/${name}` }, OPENAPI_ID, body, name);
  }

  /** Picks the AsyncAPI message from the event's own `type` and validates the whole event. */
  validateEvent(event: unknown): ValidationResult {
    const type = (event as { type?: unknown } | null)?.type;
    const message = Object.values(this.asyncapi.components.messages).find((m) => m.name === type);
    const against = `event ${String(type)}`;
    if (!message) {
      return { valid: false, against, errors: [`type ${JSON.stringify(type)} is not a message in asyncapi.yaml`] };
    }
    return this.run(`event:${message.name}`, message.payload, ASYNCAPI_ID, event, against);
  }

  private findOperation(operationId: string): Json | undefined {
    for (const item of Object.values(this.openapi.paths)) {
      for (const method of HTTP_METHODS) {
        const operation = item[method];
        if (operation && operation.operationId === operationId) return operation;
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
