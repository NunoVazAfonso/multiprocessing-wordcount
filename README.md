# Readme  

Please run `docker-compose up` to setup your environment.  

A link will be provided in the terminal similar, to access jupyter notebook within the ds-notebook-container, similar to `http://127.0.0.1:8888/?token=<YOUR_TOKEN>`  
The project contains a postgres and a jupyter notebook container.  


#### SQL Exercise.ipynb  
Please execute each cell to see effective results.  
Contains faker data to simulate satisfaction scores, to provide greater richness to the analysis.  

#### Python Exercise.ipynb  
Tests to compare serial (regular) count approach (baseline), Multiprocessing (requested) and Multithreading approaches to wordcount.   
Please find different size inputs under the `input/` folder to experiment different file size scenarios.  

#### wordcount.py  
Python program implementing multiprocessing wordcount.  
Takes **optional** text filepath argument. Defaults to `input/sample20.txt`.   

Please run in terminal using `python wordcount.py <TEXT_FILEPATH>` or simply `python wordcount.py`  
Example: `python wordcount.py input/sample20.txt`  
  
Outputs same name file containing wordcount under `output/` folder.  
Example: `ouput/sample20.txt`  