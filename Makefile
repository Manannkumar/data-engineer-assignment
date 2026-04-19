include .env
export

up:
	docker-compose up -d

down:
	docker-compose down

messages:
	go run message_generator.go

run:
	python src/etl.py

show:
	docker exec -it data-engineer-assignment-interview-db-1 psql -U etl_user -d etl_db -c "SELECT * FROM trips;"

all: up messages run show