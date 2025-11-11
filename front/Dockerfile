FROM nginx:alpine

COPY . /usr/share/nginx/html

COPY .env /app/.env
RUN BACKEND_URL=$(grep BACKEND_URL /app/.env | cut -d '=' -f2) \
    && sed -i "s|{{BACKEND_URL}}|$BACKEND_URL|g" /app/env.js

EXPOSE 5500:80

CMD ["nginx", "-g", "daemon off;"]