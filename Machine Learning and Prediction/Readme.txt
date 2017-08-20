[TennisPred_DataSet.csv]
Contains all the required data.
10 variables have been chosen for prediction.
1 output variable (win/loss)

The data has been obtained from the original combined data.

[TennisLogisticRegre_PredictWinner_Train.py]
This script is used to train and save a logistic regression model.

[TennisLogisticRegreModel.pkl]
This is the saved logistic regression model.

[TennisPredWinner_Test.py]
This script has a UI in which certain details have to be entered post which the probability of winning will be shown.
The saved model is used to predict.

Whoever is going to show the demo, in order to run this, please make sure you have numpy and plotly installed.
Just use "sudo pip install numpy" to install the same if you are not a superuser.

Also mapping for the surface:
Carpet - 0
Clay - 1
Grass - 2
Hard - 3

Test Case:
101649
100800
1
0
41
25
4
5
1
1

Alternate IDS which can be used
103819 - Roger Federer
104745 - Nadal
104925 - Djoko
103970 - David Ferrer
104918 - Andy Murray
