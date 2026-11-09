COMPOSE_TEST := docker compose -f docker-compose.test.yml

.PHONY: up down logs test

## up: build Panoptes and start it with NATS and Postgres in the background; the API is on :8080
up:
	docker compose up -d --build --wait

## down: stop and remove the stack started by `make up`
down:
	docker compose down --remove-orphans

## logs: follow the service's logs
logs:
	docker compose logs -f panoptes

## test: run every test in containers against a fresh NATS and Postgres, then remove them
test:
	$(COMPOSE_TEST) run --rm test; status=$$?; $(COMPOSE_TEST) down --remove-orphans; exit $$status
