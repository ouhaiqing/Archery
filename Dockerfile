#基于archery 1.7.0开发
#FROM hhyo/archery:1.7.0
FROM hhyo/archery:latest

COPY ./ /opt/archery/

WORKDIR /opt/archery

#archery， 添加模块需要重新执行
RUN cd /opt/archery \
    && /opt/venv4archery/bin/pip3 install -r /opt/archery/requirements_new.txt

#port
EXPOSE 9123

#start service