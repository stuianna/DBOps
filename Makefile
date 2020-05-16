VENV_NAME = venv
VENV_ACTIVATE = $(VENV_NAME)/bin/activate

test: venv
	@coverage run -m unittest discover -s tests
	@coverage report

package:
	python3 setup.py sdist bdist_wheel

release:
	python3 -m twine upload --skip-existing dist/*

install:
	pip3 install --user .

venv: $(VENV_ACTIVATE)

$(VENV_ACTIVATE): requirements.txt
	@test -d $(VENV_NAME) || virtualenv -p python3 $(VENV_NAME)
	@. $(VENV_ACTIVATE); pip3 install -r requirements.txt
	@touch $(VENV_ACTIVATE)

clean:
	@rm -rf $(VENV_NAME)
	@rm -rf *.egg-info
	@rm -rf build/
	@rm -rf dist/
	@rm -rf __pycache__
	@rm -rf **/__pycache__
	@rm -rf htmlcov
