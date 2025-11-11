FROM nginx:alpine

WORKDIR /usr/share/nginx/html
COPY . .

RUN BACKEND_URL=$(grep BACKEND_URL .env | cut -d "=" -f2) \
  && sed -i "s|{{BACKEND_URL}}|$BACKEND_URL|g" env.js

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]