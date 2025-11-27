from ETL.etl_pipeline import Pipeline
from ETL.Extractors.xml_strategy import XMLDataHandler
from ETL.Transofrm.manipulation import TransformManipulation
from ETL.Load.load import Load
from ETL.DataHandlers.FilesDataHandler.csv_handler import CSVDataHandler


# הגדר טרנספורמציה
def transform(record):
    return record


# יצור pipeline
extractor = XMLDataHandler("input.csv")
manipulation = TransformManipulation(transform)
load_handler = Load(CSVDataHandler("output.csv"))

pipeline = Pipeline(extractor, manipulation, load_handler)
pipeline.run()
