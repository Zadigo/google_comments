FROM selenium/selenium/standalone-edge:latest as builder

EXPOSE 4444

FROM python:latest

COPY . .

CMD [ "python", "-m", "base_server" ]
