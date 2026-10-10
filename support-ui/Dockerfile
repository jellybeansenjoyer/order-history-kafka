FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
COPY . .
ARG VITE_SUPPORT_API_URL=http://localhost:8102
ARG VITE_CHECKOUT_API_URL=http://localhost:8101
ARG VITE_FRAUD_API_URL=http://localhost:8103
ENV VITE_SUPPORT_API_URL=$VITE_SUPPORT_API_URL
ENV VITE_CHECKOUT_API_URL=$VITE_CHECKOUT_API_URL
ENV VITE_FRAUD_API_URL=$VITE_FRAUD_API_URL
RUN npm run build

FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
