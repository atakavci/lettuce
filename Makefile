SHELL := /bin/bash
PATH := ./work/redis-git/src:${PATH}
ROOT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
PROFILE ?= ci
SUPPORTED_TEST_ENV_VERSIONS := 8.4 8.2 8.0 7.4 7.2
DEFAULT_TEST_ENV_VERSION := 8.2
REDIS_ENV_WORK_DIR := $(or ${REDIS_ENV_WORK_DIR},$(ROOT_DIR)/work)

# Test filter variables
ifdef FILTER_UT
	FILTER_UT_ARG := -Dsurefire.failIfNoSpecifiedTests=false -Dtest=$(FILTER_UT)
else
	FILTER_UT_ARG :=
endif

ifdef FILTER_IT
	FILTER_IT_ARG := -Dfailsafe.failIfNoSpecifiedTests=false -Dit.test=$(FILTER_IT)
else
	FILTER_IT_ARG :=
endif

start:
	@if [ -z "$(version)" ]; then \
		version=$(arg); \
		if [ -z "$$version" ]; then \
			version="$(DEFAULT_TEST_ENV_VERSION)"; \
		fi; \
	fi; \
	if ! echo "$(SUPPORTED_TEST_ENV_VERSIONS)" | grep -qw "$$version"; then \
		echo "Error: Invalid version '$$version'. Supported versions are: $(SUPPORTED_TEST_ENV_VERSIONS)."; \
		exit 1; \
	fi; \
	echo "Version: $(version)"; \
    default_env_file="src/test/resources/docker-env/.env"; \
	custom_env_file="src/test/resources/docker-env/.env.v$$version"; \
	env_files="--env-file $$default_env_file"; \
	if [ -f "$$custom_env_file" ]; then \
		env_files="$$env_files --env-file $$custom_env_file"; \
	fi; \
	echo "Environment work directory: $(REDIS_ENV_WORK_DIR)"; \
	rm -rf "$(REDIS_ENV_WORK_DIR)"; \
	mkdir -p "$(REDIS_ENV_WORK_DIR)"; \
	docker compose $$env_files -f src/test/resources/docker-env/docker-compose.yml --parallel 1 up -d; \
	echo "Started test environment with Redis version $$version."


test:
	mvn -DskipITs=false clean compile verify -P$(PROFILE) $(FILTER_UT_ARG) $(FILTER_IT_ARG)

test-coverage:
	mvn -DskipITs=false clean compile verify jacoco:report -P$(PROFILE) $(FILTER_UT_ARG) $(FILTER_IT_ARG)

stop:
	docker compose --env-file src/test/resources/docker-env/.env -f src/test/resources/docker-env/docker-compose.yml down; \
	rm -rf "$(REDIS_ENV_WORK_DIR)"

clean:
	rm -Rf target/

release:
	mvn release:clean
	mvn release:prepare
	mvn release:perform
	ls target/checkout/target/*-bin.zip | xargs gpg -b -a
	ls target/checkout/target/*-bin.tar.gz | xargs gpg -b -a
