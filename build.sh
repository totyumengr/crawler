#!/bin/bash

current_dir=$(cd $(dirname $0); pwd)

common_project=${current_dir}/crawler-common
echo "----------------"
echo $common_project
echo "----------------"
echo "1.maven-build"
cd $common_project
mvn clean install

seimi_project=${current_dir}/../SeimiCrawler
echo "----------------"
echo $seimi_project
echo "----------------"
echo "1.maven-build"
cd $seimi_project
mvn clean install -DskipTests=true

emulator_project=${current_dir}/crawler-emulator
echo "----------------"
echo $emulator_project
echo "----------------"
echo "1.maven-build"
cd $emulator_project
mvn clean package
echo "2.docker-clear"
docker container ls -a | grep totyumengr/crawler-emulator | awk '{print $1}' | uniq | xargs -I {} docker rm --force {}
docker images | grep totyumengr/crawler-emulator | awk '{print $3}' | uniq | xargs -I {} docker rmi --force {}
jar=`find ./target -type f -regex '.*.jar'`
jar=${jar##*-}
jar=${jar%.*}
jar_version=$jar
echo "3.docker-build"
docker build -t totyumengr/crawler-emulator:v${jar_version} .
echo "4.docker-run"
docker run -e JAVA_OPTS='-server -Dfetcher.emulator.remoteaddress=http://host.docker.internal:4444/wd/hub -Dspring.redis.host=host.docker.internal' \
    -d --name docker-crawler-emulator totyumengr/crawler-emulator:v${jar_version}
echo "5.done"

fetcher_project=${current_dir}/crawler-fetcher
echo "----------------"
echo $fetcher_project
echo "----------------"
echo "1.maven-build"
cd $fetcher_project
mvn clean package
echo "2.docker-clear"
docker container ls -a | grep totyumengr/crawler-fetcher | awk '{print $1}' | uniq | xargs -I {} docker rm --force {}
docker images | grep totyumengr/crawler-fetcher | awk '{print $3}' | uniq | xargs -I {} docker rmi --force {}
jar=`find ./target -type f -regex '.*.jar'`
jar=${jar##*-}
jar=${jar%.*}
jar_version=$jar
echo "3.docker-build"
docker build -t totyumengr/crawler-fetcher:v${jar_version} .
echo "4.docker-run"
docker run -e JAVA_OPTS='-server -Dspring.redis.host=host.docker.internal -Dbacklog.proxy.authName= -Dbacklog.proxy.authPassword= -Dfetcher.ippool.url=http://jdmksp.v4.dailiyun.com/query.txt?key=&word=&count=5&rand=true&detail=false' \
    -d --name docker-crawler-fetcher totyumengr/crawler-fetcher:v${jar_version}
echo "5.done"

extractor_project=${current_dir}/crawler-extractor
echo "----------------"
echo $extractor_project
echo "----------------"
echo "1.maven-build"
cd $extractor_project
mvn clean package
echo "2.docker-clear"
docker container ls -a | grep totyumengr/crawler-extractor | awk '{print $1}' | uniq | xargs -I {} docker rm --force {}
docker images | grep totyumengr/crawler-extractor | awk '{print $3}' | uniq | xargs -I {} docker rmi --force {}
jar=`find ./target -type f -regex '.*.jar'`
jar=${jar##*-}
jar=${jar%.*}
jar_version=$jar
echo "3.docker-build"
docker build -t totyumengr/crawler-extractor:v${jar_version} .
echo "4.docker-run"
docker run -e JAVA_OPTS='-server -Dextractor.period=2 -Dspring.redis.host=host.docker.internal' \
    -d --name docker-crawler-extractor totyumengr/crawler-extractor:v${jar_version}
echo "5.done"

worker_project=${current_dir}/crawler-planner
echo "----------------"
echo $worker_project
echo "----------------"
echo "1.maven-build"
cd $worker_project
mvn clean package
echo "2.docker-clear"
docker container ls -a | grep totyumengr/crawler-worker | awk '{print $1}' | uniq | xargs -I {} docker rm --force {}
docker images | grep totyumengr/crawler-worker | awk '{print $3}' | uniq | xargs -I {} docker rmi --force {}
jar=`find ./target -type f -regex '.*.jar'`
jar=${jar##*-}
jar=${jar%.*}
jar_version=$jar
echo "3.docker-build"
docker build -t totyumengr/crawler-worker:v${jar_version} .
echo "4.docker-run"
docker run -e JAVA_OPTS='-server -Dexporter.story.dir=/usr/local/crawler-data -Dspring.redis.host=host.docker.internal' \
    -v /Users/mengran7/Downloads/03Data/crawler-data:/usr/local/crawler-data -d \
    --name docker-crawler-worker totyumengr/crawler-worker:v${jar_version}
echo "5.done"
