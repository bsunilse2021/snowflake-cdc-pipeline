FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml requirements.txt ./
COPY src ./src
COPY sql ./sql
RUN pip install --no-cache-dir -e .
ENTRYPOINT ["sfpipe"]
CMD ["status"]
