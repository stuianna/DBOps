test:
	@coverage run -m unittest discover -s tests
	@coverage report

package:
	python3 setup.py sdist bdist_wheel

release:
	python3 -m twine upload --repository-url https://test.pypi.org/legacy/ --skip-existing dist/*

install:
	pip3 install --user .
