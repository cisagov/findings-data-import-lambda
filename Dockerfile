FROM lambci/lambda:build-python3.8
MAINTAINER Matthew Zackschewski <matthew.zackschewski@trio.dhs.gov>
LABEL vendor="Cyber and Infrastructure Security Agency"

COPY build.sh .

# Files needed to install local fdi module.
COPY setup.py .
COPY requirements.txt .
COPY README.md .
COPY fdi ./fdi

COPY lambda_handler.py .

ENTRYPOINT ["./build.sh"]
