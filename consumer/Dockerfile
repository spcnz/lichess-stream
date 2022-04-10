FROM python:3.8.10
WORKDIR /code
#ENV HTTP_PROXY=http://ftn.proxy:8080
#ENV http_proxy=http://ftn.proxy:8080
#ENV HTTPS_PROXY=http://ftn.proxy:8080
#ENV https-proxy=http://ftn.proxy:8080
#RUN pip --proxy=http://ftn.proxy:8080 install poetry
RUN pip  install poetry
COPY ./poetry.lock pyproject.toml /code/
RUN poetry config virtualenvs.create false
RUN poetry install
COPY . /code/
EXPOSE 3000
ENTRYPOINT ["poetry", "run", "faust", "-A", "consumer", "worker", "-l", "info"]