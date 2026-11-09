COMPOSE_TEST := docker compose -f docker-compose.test.yml

.PHONY: up down logs test demo

## up: build Panoptes and start it with NATS and Postgres in the background; the API is on :8080
up:
	docker compose up -d --build --wait

## down: stop and remove the stack started by `make up`, including its data
down:
	docker compose down --volumes --remove-orphans

## logs: follow the service's logs
logs:
	docker compose logs -f panoptes

## test: run every test in containers against a fresh NATS and Postgres, then remove them
test:
	$(COMPOSE_TEST) run --rm test; status=$$?; $(COMPOSE_TEST) down --remove-orphans; exit $$status

## demo: publish a demo scenario to the stack started by `make up`, print the announcements and what to look at
demo:
	docker compose --profile demo run --rm --build demo
