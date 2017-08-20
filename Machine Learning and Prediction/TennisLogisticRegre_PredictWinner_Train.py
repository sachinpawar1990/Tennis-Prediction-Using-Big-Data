from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
import os
import pickle

# Load and parse the data
def parseTennisData(line):
    values = [float(x) for x in line.split(',')]
    return LabeledPoint(values[10], values[0:9]) #labels and features from the data

sc = SparkContext()

# Reading the csv file containing all the training data
Tennisdata = sc.textFile("file:/home/training/CA/TennisPred_DataSet.csv")
parsedTennisData = Tennisdata.map(parseTennisData) # Parsing the data into the required format for Logit

# Building the Logistic Regression model
model = LogisticRegressionWithLBFGS.train(parsedTennisData)
print(model) # Prints the weights and coeffs of the model

# Saving the trained Model
with open("/home/training/CA/TennisLogisticRegreModel.pkl", 'wb') as f:
	pickle.dump(model,f)


# Evaluating the model on training data to obtain the training error
labelsAndPreds = parsedTennisData.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedTennisData.count())
print("Training Error = " + str(trainErr))

# Obtain the training summary of the trained model
#trainingSummary = model.summary

# Obtaining the ROC of the trained model and showing the same
#trainingSummary.roc.show()
#print("areaUnderROC: " + str(trainingSummary.areaUnderROC))

