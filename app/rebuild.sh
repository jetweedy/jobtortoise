#!/bin/bash

docker stop jetstocks
docker rm jetstocks
docker rmi jetstocks

#sudo chown -R ec2-user:ec2-user .
#chmod -R u+rwX .
#chmod -R 755 templates/
#chmod -R 755 static/

docker build --no-cache -t jetstocks .

docker run -d -p 5000:5000 -v $(pwd)/../data:/data -e SQLITE_DB_PATH=/data/app.db --user $(id -u):$(id -g) --name jetstocks jetstocks

