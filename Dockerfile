#FROM python:3
FROM tiagopeixoto/graph-tool
WORKDIR /Kariz
ADD . /Kariz
RUN pacman -Syyu --noconfirm
RUN pacman-db-upgrade
RUN pacman -S python-pip --noconfirm
#RUN && apt-get update -y \
    #&& apt-get install -y python3-pip
#RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt
ENV PYTHONPATH "${PYTHONPATH}:/Kariz/code"
#ENTRYPOINT [ "python3" ]
CMD [ "python3", "./code/controller/server.py" ]
