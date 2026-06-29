FROM nginx:1.25-alpine
COPY nginx/nginx.conf /etc/nginx/nginx.conf
# Self-signed cert is generated at build time for local dev.
# For production, mount real certs at /etc/nginx/ssl/cert.pem and /etc/nginx/ssl/key.pem
RUN apk add --no-cache openssl && \
    mkdir -p /etc/nginx/ssl && \
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
      -keyout /etc/nginx/ssl/key.pem \
      -out /etc/nginx/ssl/cert.pem \
      -subj "/CN=distributed-log-system" 2>/dev/null
EXPOSE 80 443
CMD ["nginx", "-g", "daemon off;"]
