/** Where the services under test live. Defaults match `npm run start:*`. */
export const config = {
  loyaltyUrl: process.env.LOYALTY_URL ?? 'http://localhost:3001',
} as const;
