

all: clean update_readme library_to_s3 etl_scripts_to_s3

library_to_s3_dev:
	cd ./src/athena2pyspark
	zip -r ./../../athena2pyspark *
	aws s3 cp ./athena2pyspark.zip s3://dev.library.exalitica.com/
	rm ./athena2pyspark.zip
	cd ..
	cd ..

egg:
	make clean
	(cd src; zip -r ./../athena2pyspark.egg athena2pyspark/*)

	

etl_scripts_to_s3:
	# aws s3 cp --recursive ./etl_scripts s3://cencosud.exalitica.com/prod/etl_scripts/

clean:
	find . -name \*~ -delete
	find . -name \*.pyc -delete

update_readme:
	jupyter-nbconvert --to markdown README.ipynb

software_quality:
	#autogenerar documentacion de la libreria, coverade, testing, autoformat pep-8
