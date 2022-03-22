FROM python:latest
WORKDIR /code
RUN pip install poetry
COPY ./poetry.lock pyproject.toml /code/
RUN poetry config virtualenvs.create false
RUN poetry install
COPY . /code/
ENTRYPOINT ["poetry", "run", "python", "producer.py"]