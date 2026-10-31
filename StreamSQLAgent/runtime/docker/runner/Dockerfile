FROM python:3.11.13-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN python -m pip install --no-cache-dir confluent-kafka==2.11.1
WORKDIR /workspace
RUN useradd --create-home --uid 10001 runner
USER runner
CMD ["sleep", "infinity"]
