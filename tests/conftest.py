import os
import tempfile

# Tests provide their own environment and must not inherit .env.
os.environ["AIOPS_SKIP_DOTENV"] = "1"

# Must be set before any service module imports config.settings.
_TMP_DIR = tempfile.mkdtemp(prefix="aiops-tests-")
os.environ["DATABASE_URL"] = f"sqlite:///{_TMP_DIR}/test.db"
os.environ["NVIDIA_API_KEY"] = ""
os.environ["NVIDIA_MODELS"] = "glm-5-3,glm-5-3-flash,kimi-k3,muse-glimmer-30b"
os.environ["MODEL_FAILURE_THRESHOLD"] = "3"
os.environ["LOG_LEVEL"] = "WARNING"
