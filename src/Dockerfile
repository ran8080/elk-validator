FROM python:2.7.14

# Create workdir
COPY . /app
WORKDIR /app

# Create output and logs default dirs and install packages
RUN mkdir /output
RUN mkdir /logs
RUN mkdir /res

# Configure time zone for logger
RUN echo "Asia/Jerusalem" > /etc/timezone
RUN dpkg-reconfigure -f noninteractive tzdata

# Install python packages
RUN pip install -r requirements.txt

# Run python program
CMD ["python", "/src/modules/main.py", "-c", "src/config/validator.yaml"]
