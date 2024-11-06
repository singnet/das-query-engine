FROM docker:20.10.21-dind-alpine3.16
RUN apk add python3 --update --no-cache python3 && ln -sf python3 /usr/bin/python
RUN apk add git
RUN apk add bash
RUN apk add curl
RUN python3 -m ensurepip
WORKDIR /das
RUN pip3 install --no-cache --upgrade pip setuptools
RUN pip3 install hyperon-das
RUN git clone https://github.com/singnet/das-toolbox.git
RUN pip3 install -r das-toolbox/src/requirements.txt
COPY config.json /root/.das/config.json
COPY entrypoint.sh entrypoint.sh
COPY animals.metta animals.metta
EXPOSE 8080
CMD ["/bin/bash","-c","./entrypoint.sh"]
