FROM python:3.8
LABEL org.opencontainers.image.source https://github.com/SENERGY-Platform/consumption_profile
WORKDIR /usr/src/app
COPY . .
RUN apt-get update && apt-get install -y git
RUN git log -1 --pretty=format:"commit=%H%ndate=%cd%n" > git_commit 
RUN python3 -m pip install --no-cache-dir -r requirements.txt 
RUN apt-get purge -y git && apt-get auto-remove -y && apt-get clean && rm -rf .git
CMD [ "python3", "-u", "main.py"]
