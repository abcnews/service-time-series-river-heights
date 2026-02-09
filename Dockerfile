# Use Node 24 as the base image
FROM node:24-slim

# Install curl for the get-db script
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --omit=dev

# Copy the rest of the application code
COPY . .

# Ensure data directory exists
RUN mkdir -p data

# Default command to run the scheduler
CMD ["npm", "run", "scheduler"]
