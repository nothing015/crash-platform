# 1. Use your existing Spark image as the base
FROM spark:4.1.1-python3

# 2. Switch to root user to install packages
USER root

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

# 4. Switch back to the default Spark user for security (usually 1001 for bitnami images)
USER 1001