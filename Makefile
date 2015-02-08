flake:
	flake8 main.py mapreduce.py ./tests/tests.py 
	
clean:
	rm -f `find . -type f -name '*.py[co]'`

