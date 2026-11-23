# Loaded automatically by gunicorn from the working directory (backend/).
# Threads let the API answer progress polls and lookups while a scan runs in the
# background. Command-line flags (e.g. Railway's start command) still override these.
worker_class = "gthread"
workers = 1
threads = 8
timeout = 300
