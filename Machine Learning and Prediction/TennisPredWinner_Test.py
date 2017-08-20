from Tkinter import *
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
import os
import pickle
import plotly
import plotly.graph_objs as go

plotly.offline.init_notebook_mode()

root = Tk()
toolbar = Frame(root)

# Function to predict the probability from the provided data.
def predictWinner():
	ProbPred = [int(SurfText.get()),int(Ply1Text.get()),int(Ply2Text.get()),int(DfText.get()),int(svPtText.get()), 			   int(FirServeWonText.get()),int(SecServeWonText.get()),int(srvGamesText.get()), int(BrkPtSavedText.get())]
	print(ProbPred)
	model = pickle.load(open("/home/training/CA/TennisLogisticRegreModel.pkl","rb"))
	model.clearThreshold()
	winProb = model.predict(ProbPred)
	print(winProb)

	# Plotting the probabilities on a bar chart
	data = [go.Bar(
            x=[int(Ply1Text.get()),int(Ply2Text.get())],
            y=[winProb*100, ((1-winProb)*100)]
    	)]

	layout = go.Layout(
        title='Tennis Winner Probability Prediction',
        xaxis=dict(
	title='Player ID',
	ticks='',
        showticklabels=False,
	titlefont=dict(
	    family='Courier New, monospace',
	    size=18,
	    color='#7f7f7f'
	    )
          ),
        yaxis=dict(
        title='Winning Probability %',
        titlefont=dict(
            family='Courier New, monospace',
            size=18,
            color='#7f7f7f'
            )
          )
        )
	fig = go.Figure(data=data, layout = layout)
	plotly.offline.plot(fig)
	
HeadingLabel = Label(root,text="Tennis Winner Prediction System",font =("Italic",18,"bold")).grid(row=0)

Ply1label = Label(root,text="Player (ID)",font =("Italic",10)).grid(row=1)
Ply1ID=StringVar(None)
Ply1Text=Entry(root,textvariable=Ply1ID,width=50)
Ply1Text.grid(row=1, column =1)

#Ply1DropDwn_Var = StringVar()
#Ply1DropDwn = OptionMenu(root,Ply1DropDwn_Var,'Option1','Option2','Option3')
#Ply1DropDwn.pack(side = RIGHT)

Ply2label = Label(root,text="Opponent (ID)",font =("Italic",10)).grid(row=2)
Ply2ID=StringVar(None)
Ply2Text=Entry(root,textvariable=Ply2ID,width=50)
Ply2Text.grid(row=2, column =1)

Surflabel = Label(root,text="Surface",font =("Italic",10)).grid(row=3)
Surf=StringVar(None)
SurfText=Entry(root,textvariable=Surf,width=50)
SurfText.grid(row=3, column =1)

Dflabel = Label(root,text="Double Faults",font =("Italic",10)).grid(row=4)
Df=StringVar(None)
DfText=Entry(root,textvariable=Df,width=50)
DfText.grid(row=4, column =1)

svPtlabel = Label(root,text="Service Points",font =("Italic",10)).grid(row=5)
svPt=StringVar(None)
svPtText=Entry(root,textvariable=svPt,width=50)
svPtText.grid(row=5, column =1)

FirServeWonlabel = Label(root,text="First Serves Won",font =("Italic",10)).grid(row=6)
FirServeWon=StringVar(None)
FirServeWonText=Entry(root,textvariable=FirServeWon,width=50)
FirServeWonText.grid(row=6, column =1)

SecServeWonlabel = Label(root,text="Second Serves Won",font =("Italic",10)).grid(row=7)
SecServeWon=StringVar(None)
SecServeWonText=Entry(root,textvariable=SecServeWon,width=50)
SecServeWonText.grid(row=7, column =1)

srvGameslabel = Label(root,text="Service Games",font =("Italic",10)).grid(row=8)
srvGames=StringVar(None)
srvGamesText=Entry(root,textvariable=srvGames,width=50)
srvGamesText.grid(row=8, column =1)

BrkPtSavedlabel = Label(root,text="Break Points Saved",font =("Italic",10)).grid(row=9)
BrkPtSaved=StringVar(None)
BrkPtSavedText=Entry(root,textvariable=BrkPtSaved,width=50)
BrkPtSavedText.grid(row=9, column =1)

BrkPtFacedlabel = Label(root,text="Break Points Faced",font =("Italic",10)).grid(row=10)
BrkPtFaced=StringVar(None)
BrkPtFacedText=Entry(root,textvariable=BrkPtFaced,width=50)
BrkPtFacedText.grid(row=10, column =1)

SubButton = Button(root, text="Predict Winner", width=13, command = predictWinner).grid(row=11)


mainloop()
