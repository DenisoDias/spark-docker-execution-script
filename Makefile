
start:
	echo "Starting spark locally"
	docker-compose up -d
stop:
	echo "Stopping spark"
	docker-compose down
