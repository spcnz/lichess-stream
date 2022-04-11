FROM node:13.12.0-alpine
WORKDIR /app
ENV PATH /app/node_modules/.bin:$PATH
ENV HTTP_PROXY=http://proxy.uns.ac.rs:8080
ENV http_proxy=http://proxy.uns.ac.rs:8080
ENV HTTPS_PROXY=http://proxy.uns.ac.rs:8080
ENV https-proxy=http://proxy.uns.ac.rs:8080
COPY package.json ./
COPY package-lock.json ./
RUN npm --proxy=http://proxy.uns.ac.rs:8080 install --silent
#RUN npm  install react-scripts@3.4.0 -g --silent
COPY . ./
CMD ["npm", "start"]