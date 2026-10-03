FROM node:20-alpine
RUN apk add --no-cache unzip
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm install --production
COPY app.js ./
EXPOSE 5000
CMD ["node", "app.js"]
