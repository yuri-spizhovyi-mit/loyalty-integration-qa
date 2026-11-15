# One image for all four mock services; docker-compose.yml picks the service
# with SERVICE. The services are small TypeScript programs run with tsx, so
# there is no build step.
FROM node:24.14-alpine

WORKDIR /app
ENV NODE_ENV=production

COPY package.json package-lock.json ./
# tsx (which runs the services) is a dev dependency, so dev dependencies are
# included. Optional ones are too: tsx needs esbuild's platform binary.
RUN npm ci --include=dev --no-audit --no-fund \
 && npm cache clean --force

COPY tsconfig.json ./
COPY services ./services

USER node
ENV SERVICE=loyalty-engine
CMD ["sh", "-c", "exec node_modules/.bin/tsx services/${SERVICE}/src/main.ts"]
