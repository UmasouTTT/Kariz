#FROM python:3
FROM tiagopeixoto/graph-tool
WORKDIR /Kariz
ADD . /Kariz
RUN pacman -Syyu --noconfirm
RUN pacman-db-upgrade
RUN pacman -S python-pip --noconfirm
RUN pip3 install -r requirements.txt
RUN pip3 install 'connexion[swagger-ui]'
#RUN pip3 install pathlib swagger_ui_bundle
ENV PYTHONPATH "${PYTHONPATH}:/Kariz/code"
CMD [ "python3", "./code/controller/server.py" ]
