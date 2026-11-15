import type { APIResponse } from '@playwright/test';

/** Parses a response body as T. The type is a claim; schema checks (step 3) make it a fact. */
export async function readJson<T>(response: APIResponse): Promise<T> {
  return (await response.json()) as T;
}

export const ISO_8601_UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$/;
