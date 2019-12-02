FROM hhyo/archery:latest

COPY ./ /opt/archery/

#port
EXPOSE 9123

#start service
ENTRYPOINT bash /opt/archery/src/docker/startup.sh && bash