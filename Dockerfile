FROM node:22-alpine

ENV NODE_ENV=production
WORKDIR /app

# Install exact dependency versions from package-lock.json
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY src ./src
COPY frontend /static

USER node
EXPOSE 8080

# The API. node:cluster starts WEB_PROCESSES event loops in this container.
CMD ["node", "src/api.js"]
