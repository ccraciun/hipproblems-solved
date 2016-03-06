.PHONY: test clean

repl: env
	. env/bin/activate; ipython

clean:
	python setup.py clean
	find searchrunner -type f -name "*.pyc" -exec rm {} \;
	find workqueue -type f -name "*.pyc" -exec rm {} \;

purge: clean
	rm -rf env dist build *.egg-info

env: env/bin/activate

env/bin/activate: setup.py
	test -d env || virtualenv -p python --no-site-packages env
	. env/bin/activate ; pip install -U pip wheel
	. env/bin/activate ; python setup.py install
	touch env/bin/activate
