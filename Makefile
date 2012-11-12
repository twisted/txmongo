# Unit-testing, docs, etc.

VIRTUALENV?=virtualenv

all: test flakes pep8

env:
	rm -fr env
	mkdir -p .download_cache
	$(VIRTUALENV) --no-site-packages env
	env/bin/pip install --download-cache=.download_cache Twisted epydoc pyflakes pep8
	echo "\n\n>> Run 'source env/bin/activate'"

docs:
	rm -fr docs
	mkdir -p docs
	env/bin/epydoc -v --html --output=docs txmongo

test:
	env/bin/trial tests

coverage:
	env/bin/trial --coverage tests

flakes:
	-env/bin/pyflakes txmongo

pep8:
	-env/bin/pep8 --ignore=E501 -r txmongo


.PHONY: env docs
