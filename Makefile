test:
	@coverage run -m unittest discover -s tests
	@coverage report
