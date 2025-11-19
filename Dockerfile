# Use the official Python image
FROM python:3.12-slim

WORKDIR /app

# Set the timezone to Japan Standard Time at the very beginning
ENV TZ=Asia/Tokyo

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies including tzdata for timezone support
RUN apt-get update && apt-get install -y \
    cron \
    curl \
    tzdata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configure timezone properly for cron
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code
COPY . .

# Copy the startup script
COPY start.sh /app/start.sh
COPY cron_job_ibd.sh /app/cron_job_ibd.sh
COPY cron-env.sh /app/cron-env.sh

# Make scripts executable
RUN chmod +x /app/start.sh
RUN chmod +x /app/cron_job_ibd.sh
RUN chmod +x /app/cron-env.sh

# Add cron job with explicit timezone
# Important: Include TZ in the crontab itself
# Run at 6:30 AM JST, Monday to Friday
RUN ( \
    echo "SHELL=/bin/bash" ; \
    echo "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" ; \
    echo "TZ=Asia/Tokyo" ; \
    echo "" ; \
    echo "30 6 * * 1-5 . /app/cron-env.sh && /app/cron_job_ibd.sh >> /app/logs/cron_error.log 2>&1" \
) | crontab -

# Create logs directory
RUN mkdir -p /app/logs

# Start services using the startup script
CMD ["/app/start.sh"]
