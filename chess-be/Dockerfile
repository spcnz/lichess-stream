FROM python:3.8.10
WORKDIR /code
RUN pip  install poetry
COPY ./poetry.lock pyproject.toml /code/
RUN poetry config virtualenvs.create false
RUN poetry install
COPY . /code/
EXPOSE 8008
ENTRYPOINT ["poetry", "run", "python", "server.py"]