# Use official Python 3.12 image
FROM python:3.14-slim-trixie

# Set a working directory inside the container
WORKDIR /app

# Copy requirements first to leverage Docker layer caching
COPY requirements.txt .

RUN apt-get update -y && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the repository (Python scripts, etc.)
COPY ./src .

# Default command for interactive use
CMD ["bash"]
