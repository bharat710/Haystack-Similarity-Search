import logging
import logging_loki

handler = logging_loki.LokiHandler(
    url="http://loki:3100/loki/api/v1/push",
    tags={"application": "haystack-file-system"},
    version="1",
)

logger = logging.getLogger("haystack-logger")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

print("Logging service started.")
