bump:
	pipenv run bump2version --allow-dirty --message ${msg} --tag-message ${msg} ${type}

lint: ## check style with flake8
	flake8 lmax_jupyter_tools tests

test: ## run tests quickly with the default Python
	python setup.py test

test-unittest: ## run all unittests
	python -m unittest discover --verbose -s tests

install: ## install the package to the active Python's site-packages
	python setup.py install
