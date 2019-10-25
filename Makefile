run:
	sudo docker-compose up -d
	sudo docker pull dajobe/hbase
	mkdir -p data
	id=$(sudo docker run --name=hbase-docker -h hbase-docker -d -v $PWD/data:/data dajobe/hbase)
	wget https://raw.githubusercontent.com/scrapinghub/hbase-docker/master/start-hbase.sh
	chmod +x start-hbase.sh
	sudo ./start-hbase.sh
	rm start-hbase.sh
