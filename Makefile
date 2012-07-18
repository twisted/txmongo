# Unit-testing, docs, etc.

VIRTUALENV?=virtualenv
# VIRTUALENV?=virtualenv-2.6
EPYDOC=epydoc
TRIAL?=trial
PYFLAKES?=pyflakes
PEP8?=pep8

all: test flakes pep8

env:
	rm -fr env
	mkdir -p .download_cache
	$(VIRTUALENV) --no-site-packages env
	env/bin/pip install --download-cache=.download_cache/ Twisted epydoc pyflakes pep8
	echo "\n\n>> Run 'source env/bin/activate'"

docs:
	rm -fr docs
	mkdir -p docs
	$(EPYDOC) -v --html --output=docs txmongo

test:
	$(TRIAL) -e tests

coverage:
	$(TRIAL) --coverage tests

flakes:
	$(PYFLAKES) txmongo

pep8:
	$(PEP8) --ignore=E501 -r txmongo


.PHONY: env docs
