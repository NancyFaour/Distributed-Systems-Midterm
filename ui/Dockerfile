FROM node:24-alpine@sha256:8a32c8cf649a49915a66e81e7a609223c708d0adabf288ee3f4077d765542f85 AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY index.html ./
COPY src ./src
RUN npm run build
FROM nginx:1.28-alpine@sha256:a8b39bd9cf0f83869a2162827a0caf6137ddf759d50a171451b335cecc87d236
COPY nginx.conf /etc/nginx/templates/default.conf.template
COPY entrypoint.sh /entrypoint.sh
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
