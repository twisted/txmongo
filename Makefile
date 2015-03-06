# Unit-testing, docs, etc.

VIRTUALENV?=virtualenv
EPYDOC=epydoc
TRIAL?=trial
COVERAGE?=coverage
PYFLAKES?=pyflakes
PEP8?=pep8

all: test flakes pep8

env:
	rm -fr env
	mkdir -p .download_cache
	$(VIRTUALENV) --no-site-packages env
	env/bin/pip install --download-cache=.download_cache/ -r requirements.txt 
	echo "\n\n>> Run 'source env/bin/activate'"

docs:
	rm -fr docs/build/*
	make -C docs html

test:
	$(TRIAL) -e tests

coverage:
	$(COVERAGE) run --source=txmongo `which $(TRIAL)` tests
	$(COVERAGE) report -m

flakes:
	$(PYFLAKES) txmongo

pep8:
	$(PEP8) --ignore=E501 -r txmongo


.PHONY: env docs
