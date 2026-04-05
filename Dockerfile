FROM python:3.12-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Create necessary directories
RUN mkdir -p database Data/gold

# Define volumes for data persistence
VOLUME ["/app/database", "/app/Data/bronze_raw", "/app/Data/silver", "/app/Data/gold"]

# Run the pipeline
CMD ["python", "main.py"]