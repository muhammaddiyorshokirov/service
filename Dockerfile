FROM node:22-bookworm-slim

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install

COPY . .
RUN npm run build

ENV NODE_ENV=production
ENV PORT=8788
ENV NODE_OPTIONS=--max-old-space-size=384

EXPOSE 8788

CMD ["npm", "start"]
