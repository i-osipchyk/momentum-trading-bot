# AWS Lambda Python 3.11 base image (ARM64)
FROM public.ecr.aws/lambda/python:3.11-arm64

# Set working directory
WORKDIR /var/task

# Copy dependency list
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy Lambda function code
# IMPORTANT: file must contain lambda_handler
COPY main.py .

# Set Lambda handler
CMD ["main.lambda_handler"]

# AWS ECR Deployment
# docker buildx build  --platform linux/arm64 -t crypto-llm-trading-bot-send-request-to-llm:v7 containers/crypto-llm-trading-bot/crypto-llm-trading-bot-send-request-to-llm
# docker tag crypto-llm-trading-bot-send-request-to-llm:v7 281324666910.dkr.ecr.eu-central-1.amazonaws.com/crypto-llm-trading-bot-send-request-to-llm:v7
# docker push 281324666910.dkr.ecr.eu-central-1.amazonaws.com/crypto-llm-trading-bot-send-request-to-llm:v7