COMPOSE_TEST := docker compose -f docker-compose.test.yml

.PHONY: test

## test: run every test in containers against a fresh NATS and Postgres, then remove them
test:
	$(COMPOSE_TEST) run --rm test; status=$$?; $(COMPOSE_TEST) down --remove-orphans; exit $$status
