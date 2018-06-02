init:
	pip install -r requirements.txt

test:
	pytest tests

functional:
	pytest tests/test_functional.py -x

unit:
	ptw starfish_shell tests \
		--ignore tests/test_functional.py


.PHONY: init test
