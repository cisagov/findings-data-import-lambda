FROM lambci/lambda:build-python3.6
MAINTAINER Matthew Zackschewski <matthew.zackschewski@trio.dhs.gov>

COPY build.sh .

# Files needed to install local adi module
COPY setup.py .
COPY requirements.txt .
COPY README.md .
COPY adi ./adi

COPY lambda_handler.py .

ENTRYPOINT ["./build.sh"]
