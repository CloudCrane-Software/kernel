# kernel-task — resident task image for runsc (gVisor) episode execution.
# WO-101 §7.3 deployment checklist: python:3.12-slim + httpx + the
# reconciler package with PYTHONPATH=/app. Keep the built image resident on
# the episode host under BOTH tags: cloudcrane/kernel-task:latest and
# kernel-task:latest (RunscRunner default image name).
#
# Build from the repo root:
#   docker build -f deploy/kernel-task.Dockerfile \
#     -t cloudcrane/kernel-task:latest -t kernel-task:latest .
FROM python:3.12-slim

RUN pip install --no-cache-dir httpx

WORKDIR /app
COPY reconciler ./reconciler
ENV PYTHONPATH=/app

# RunscRunner overrides the command with `bash task.sh`; the CMD below is
# only a safe fallback for interactive inspection.
CMD ["python3"]
