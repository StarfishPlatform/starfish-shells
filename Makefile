init:
	pip install -r requirements.txt

test:
	pytest tests

functional:
	pytest tests/test_functional.py tests/test_mock_server.py -x

unit:
	ptw starfish_shell tests \
		--ignore tests/test_functional.py \
		--ignore tests/test_mock_server.py \


.PHONY: init test
