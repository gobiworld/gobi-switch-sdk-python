
# Define variables
PYTHON = python
PIP = pip

# Default target
all: help

# Install target
install:
	$(PIP) install -r requirements.txt

docker-restart:
	docker-compose down -v
	#docker-compose down -v --remove-orphans
	docker-compose -f docker-compose.yml up --build --force-recreate -d

aws-login:
	aws sts get-caller-identity;
	aws ecr get-login-password --region eu-west-3 | \
	docker login --username AWS --password-stdin 471112719597.dkr.ecr.eu-west-3.amazonaws.com;

build:
	docker buildx build --platform linux/amd64,linux/arm64 \
	  -t 471112719597.dkr.ecr.eu-west-3.amazonaws.com/gobi-kafka-consumer:latest \
	  --push .

build-push: aws-login build
