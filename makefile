all:

init:
	sudo apt update
	sudo apt install python3 python3-venv
	python3 -m venv spark-env
	@echo "Run :"
	@echo "   source spark-env/bin/activate"
	@echo "   pip install --upgrade pip"
	@echo "   pip install pyspark"

activate:
	@echo "Run :"
	@echo "   source spark-env/bin/activate"

################
# Part 1
################

part1: part1-download part1-unzip

part1-download:
	@for file in $(shell cat files.txt | head -10); do \
		echo $${file} ;\
		if [ -f dataset/zip//$${file} ]; then \
			echo "Nothing to do" ;\
		else \
			wget http://data.gdeltproject.org/events/$${file} -P dataset/zip/;\
		fi;\
	done

part1-unzip:
	@for file in $(shell ls dataset/zip/); do \
		echo $${file} ;\
		unzip -n dataset/zip//$${file} -d dataset/raw ;\
	done

deactivate:
	@echo "Run :"
	@echo "   deactivate"

clean: deactivate
	rm -rf dataset/parquet
	rm -rf dataset/raw
	rm -rf dataset/zip
	rm -rf output/

mr-proper: clean
	rm -rf spark-env/
