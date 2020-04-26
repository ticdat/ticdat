FROM opexanalytics/r-py-gurobi:3.8.1

COPY . /src

RUN pip3.8 uninstall -y ticdat && \
	cd /src && \
	python3.8 setup.py bdist_wheel && \
	pip3.8 install dist/*.whl && \
	pip3.8 uninstall -y framework_utils && \
	pip3.8 install testing.postgresql && \
	pip3.8 install git+ssh://git@github.com/opex-analytics/python_framework_utils.git@master

CMD ["python3.8", "/src/ticdat/testing/run_tests_for_many.py"]
