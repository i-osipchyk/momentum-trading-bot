# AWS Lambda Python 3.11 base image (ARM64)
FROM public.ecr.aws/lambda/python:3.11-arm64

# Set working directory
WORKDIR /var/task

# Copy requirements first (better caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy Lambda function code
COPY main.py .

# Command for Lambda
CMD ["main.lambda_handler"]
