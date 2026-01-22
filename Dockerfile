# Use official Python 3.12 image
FROM python:3.12-slim

# Set a working directory inside the container
WORKDIR /app

# Copy requirements first to leverage Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the repository (Python scripts, etc.)
COPY ./src .

# Default command for interactive use
CMD ["bash"]
