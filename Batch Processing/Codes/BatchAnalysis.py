# This code demonstrates the batch analysis done on the data

# Importing necessary libraries
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

#sc = SparkContext()

#if __name__ == "__main__":

# create Spark context with Spark configuration

conf = SparkConf().setAppName("Spark Count")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
  
# read in file and split

tokenized = sc.textFile("/tennis/tennisData/part-m-00000")
rdd1 = tokenized.map(lambda line: line.split(','))

# RDD to DataFrame Conversion

newDF = sqlContext.createDataFrame(rdd1, ["tourney_id", "tourney_name", "surface", "draw_size", "tourney_level", "tourney_date", "tourney_day","tourney_month","tourney_year","match_num", "winner_id", "winner_seed", "winner_entry", "winner_name", "winner_hand", "winner_ht", "winner_ioc", "winner_age", "winner_rank", "winner_rank_points", "loser_id", "loser_seed", "loser_entry", "loser_name", "loser_hand", "loser_ht", "loser_ioc", "loser_age", "loser_rank", "loser_rank_points", "score", "best_of", "round", "minutes", "w_ace", "w_df", "w_svpt", "w_1stIn", "w_1stWon", "w_2ndWon", "w_SvGms","w_bpSaved","w_bpFaced", "l_ace", "l_df", "l_svpt", "l_1stIn", "l_1stWon", "l_2ndWon", "l_SvGms","l_bpSaved","l_bpFaced"])
#rdd2 = rdd1.take(2)

# Conversion of Data Type from string to respective data type

df_cs = newDF.select(newDF.tourney_id,newDF.tourney_name,newDF.surface,newDF.draw_size.cast("int"),newDF.tourney_level,newDF.tourney_date.cast("date"),newDF.tourney_day.cast("int"),newDF.tourney_month.cast("int"),newDF.tourney_year.cast("int"),newDF.match_num.cast("int"),newDF.winner_id.cast("int"),newDF.winner_seed.cast("int"),newDF.winner_entry,newDF.winner_name,newDF.winner_hand,newDF.winner_ht.cast("int"),newDF.winner_ioc,newDF.winner_age.cast("float"),newDF.winner_rank.cast("int"),newDF.winner_rank_points.cast("int"),newDF.loser_id.cast("int"),newDF.loser_seed.cast("int"),newDF.loser_entry,newDF.loser_name,newDF.loser_hand,newDF.loser_ht.cast("int"),newDF.loser_ioc,newDF.loser_age.cast("float"),newDF.loser_rank.cast("int"),newDF.loser_rank_points.cast("int"),newDF.score,newDF.best_of.cast("int"),newDF.round,newDF.minutes.cast("int"),newDF.w_ace.cast("int"),newDF.w_df.cast("int"),newDF.w_svpt.cast("int"),newDF.w_1stIn.cast("int"),newDF.w_1stWon.cast("int"),newDF.w_2ndWon.cast("int"),newDF.w_SvGms.cast("int"),newDF.w_bpSaved.cast("int"),newDF.w_bpFaced.cast("int"),newDF.l_ace.cast("int"),newDF.l_df.cast("int"),newDF.l_svpt.cast("int"),newDF.l_1stIn.cast("int"),newDF.l_1stWon.cast("int"),newDF.l_2ndWon.cast("int"),newDF.l_SvGms.cast("int"),newDF.l_bpSaved.cast("int"),newDF.l_bpFaced.cast("int"))

# Keeping the original columns name as there is change of column name after casting
 
oldcolumns = df_cs.columns
newcolumns = ["tourney_id", "tourney_name", "surface", "draw_size", "tourney_level", "tourney_date", "tourney_day","tourney_month","tourney_year","match_num", "winner_id", "winner_seed", "winner_entry", "winner_name", "winner_hand", "winner_ht", "winner_ioc", "winner_age", "winner_rank", "winner_rank_points", "loser_id", "loser_seed", "loser_entry", "loser_name", "loser_hand", "loser_ht", "loser_ioc", "loser_age", "loser_rank", "loser_rank_points", "score", "best_of", "round", "minutes", "w_ace", "w_df", "w_svpt", "w_1stIn", "w_1stWon", "w_2ndWon", "w_SvGms","w_bpSaved","w_bpFaced", "l_ace", "l_df", "l_svpt", "l_1stIn", "l_1stWon", "l_2ndWon", "l_SvGms","l_bpSaved","l_bpFaced"]
df_rnm = reduce(lambda df_cs,idx:df_cs.withColumnRenamed(oldcolumns[idx],newcolumns[idx]),xrange(len(oldcolumns)), df_cs)

# Registering the temporary table with Spark
df_rnm.registerTempTable("tennis")

# Calculation of Frequent Clay Court Winners
clay_qr = sqlContext.sql("""SELECT winner_name FROM tennis where round like "F%" and surface like "Cl%" """)
clay_rdd = clay_qr.rdd.map(list)
clay_res = clay_rdd.map(lambda y: (str(y[0]))).map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2).sortBy(lambda x: -x[1]).take(5)
#rdd2 = rdd1.filter(lambda fields:fields[29].startswith("F")).map(lambda fields: (fields[2],fields[10]))

# Calculation of Frequent Grass Court Winners
grass_qr = sqlContext.sql("""SELECT winner_name FROM tennis where round like "F%" and surface like "Gr%" """)
grass_rdd = grass_qr.rdd.map(list)
grass_res = grass_rdd.map(lambda y: (str(y[0]))).map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2).sortBy(lambda x: -x[1]).take(5)

# Calculation of Frequent Hard Court Winners
hard_qr = sqlContext.sql("""SELECT winner_name FROM tennis where round like "F%" and surface like "Ha%" """)
hard_rdd = hard_qr.rdd.map(list)
hard_res = hard_rdd.map(lambda y: (str(y[0]))).map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2).sortBy(lambda x: -x[1]).take(5)

#tot_finals = sqlContext.sql("""SELECT winner_seed FROM tennis where round like "F%" """)
#seed_rdd = tot_finals.rdd.map(list)
#seed_res = seed_rdd.map(lambda y: (str(y[0]))).map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2).collect()
#seed_distr = sqlContext.sql("""SELECT distinct winner_seed, tourney_level,count(winner_seed) FROM tennis where round like "F%" and tourney_level='G' group by winner_seed,tourney_level order by winner_seed,tourney_level,c2 desc""")

# Calculation of Players Seed Distribution in Grandslam matches
seed_distr = sqlContext.sql("""SELECT distinct winner_seed,count(winner_seed) as frequency FROM tennis where round like "F%" and tourney_level='G' group by winner_seed order by frequency desc""")
seed_distr_op = seed_distr.collect()
seed_panda=seed_distr.toPandas()

# Calculation of Age distribution of tournament winners over the years across different surfaces
aged = sqlContext.sql("""SELECT tourney_year,surface,avg(winner_age) from tennis where round like "F%" group by tourney_year,surface order by surface,tourney_year,c2 desc""")
aged_op=aged.collect()
aged_panda=aged.toPandas()


# Calculation of Match Winning Rate of players in the ATP matches
players_win = sqlContext.sql(""" SELECT winner_name FROM tennis """)
players_win_rdd = players_win.rdd.map(list)
players_win_res = players_win_rdd.map(lambda y: (str(y[0]))).map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2)
players_lose = sqlContext.sql(""" SELECT loser_name FROM tennis """)
players_lose_rdd = players_lose.rdd.map(list)
players_lose_res = players_lose_rdd.map(lambda y: (str(y[0]))).map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2)
win_lose_join = players_win_res.join(players_lose_res)
win_perc_res = win_lose_join.map(lambda (name, (win,lose)): (name, win, lose, win*100/(win+lose))).filter(lambda fields: fields[1] > 100).map(lambda fields:(fields[0],fields[3])).sortBy(lambda x: -x[1]).take(10)

# Comparison of Tournament Winners, General Winners, General Losers
win_prm = sqlContext.sql(""" SELECT avg(w_ace),avg(w_df),avg(w_svpt),avg(w_SvGms),avg(w_bpSaved),avg(w_1stWon) from tennis """)
win_prm_op = win_prm.collect()
los_prm = sqlContext.sql(""" SELECT avg(l_ace),avg(l_df),avg(l_svpt),avg(l_SvGms),avg(l_bpSaved),avg(l_1stWon) from tennis """)
los_prm_op = los_prm.collect()
tourney_win_prm = sqlContext.sql(""" SELECT avg(w_ace),avg(w_df),avg(w_svpt),avg(w_SvGms),avg(w_bpSaved),avg(w_1stWon) from tennis where round like "F%" """)
tourney_win_prm_op = tourney_win_prm.collect()

# Consolidation of above result in one dataframe
par_comp = pd.DataFrame(columns=['Parameters','Tourney Winners','Winners','Losers'])
params = ['Aces','Dbl Flts','Svc Pts','Svc Game','Brk Pts Saved','1st Svc Won']
par_comp['Parameters']=params
for i in range(len(win_prm_op[0])):
	par_comp['Winners'][i]=round(win_prm_op[0][i],2)
for i in range(len(los_prm_op[0])):
	par_comp['Losers'][i]=round(los_prm_op[0][i],2)
for i in range(len(tourney_win_prm_op[0])):
	par_comp['Tourney Winners'][i]=round(tourney_win_prm_op[0][i],2)

# Comparison of different parameters of Top players  
rog_fed = sqlContext.sql(""" SELECT avg(w_ace),avg(w_df),avg(w_svpt),avg(w_SvGms),avg(w_bpSaved),avg(w_1stWon) from tennis where winner_name="Roger Federer" """)
rog_fed_op = rog_fed.collect()
raf_nad = sqlContext.sql(""" SELECT avg(w_ace),avg(w_df),avg(w_svpt),avg(w_SvGms),avg(w_bpSaved),avg(w_1stWon) from tennis where winner_name="Rafael Nadal" """)
raf_nad_op = raf_nad.collect()
nov_djoko = sqlContext.sql(""" SELECT avg(w_ace),avg(w_df),avg(w_svpt),avg(w_SvGms),avg(w_bpSaved),avg(w_1stWon) from tennis where winner_name="Novak Djokovic" """)
nov_djoko_op = nov_djoko.collect()
and_agg = sqlContext.sql(""" SELECT avg(w_ace),avg(w_df),avg(w_svpt),avg(w_SvGms),avg(w_bpSaved),avg(w_1stWon) from tennis where winner_name="Andre Agassi" """)
and_agg_op = and_agg.collect()
pet_samp = sqlContext.sql(""" SELECT avg(w_ace),avg(w_df),avg(w_svpt),avg(w_SvGms),avg(w_bpSaved),avg(w_1stWon) from tennis where winner_name="Pete Sampras" """)
pet_samp_op = pet_samp.collect()

# Consolidation of above result in one dataframe
plyr_comp = pd.DataFrame(columns=['Parameters','Federer','Nadal','Djokovic','Aggasi','Sampras'])
params = ['Aces','Dbl Flts','Svc Pts','Svc Game','Brk Pts Saved','1st Svc Won']
plyr_comp['Parameters']=params
for i in range(len(rog_fed_op[0])):
	plyr_comp['Federer'][i]=round(rog_fed_op[0][i],2)
for i in range(len(raf_nad_op[0])):
	plyr_comp['Nadal'][i]=round(raf_nad_op[0][i],2)
for i in range(len(nov_djoko_op[0])):
	plyr_comp['Djokovic'][i]=round(nov_djoko_op[0][i],2)
for i in range(len(and_agg_op[0])):
	plyr_comp['Aggasi'][i]=round(and_agg_op[0][i],2)
for i in range(len(pet_samp_op[0])):
	plyr_comp['Sampras'][i]=round(pet_samp_op[0][i],2)

##############################################################

# Visualization for Clay Court Winners
plt.clf()
OX=[]
OY=[]
for i in range(len(clay_res)):
	OX.append(clay_res[i][0])
	OY.append(clay_res[i][1])
fig=plt.figure()
width=0.35
ind=np.arange(len(OY))
plt.bar(ind,OY,width=width)
plt.xticks(ind+width/2,OX)
fig.autofmt_xdate()
plt.savefig('/home/training/Desktop/clay_winners.png')

#############################################################

# Visualization for Grass Court Winners
plt.clf()
OX=[]
OY=[]
for i in range(len(grass_res)):
	OX.append(grass_res[i][0])
	OY.append(grass_res[i][1])
fig=plt.figure()
width=0.35
ind=np.arange(len(OY))
plt.bar(ind,OY,width=width)
plt.xticks(ind+width/2,OX)
fig.autofmt_xdate()
plt.savefig('/home/training/Desktop/grass_winners.png')

######################################################

# Visualization for Hard Court Winners
plt.clf()
OX=[]
OY=[]
for i in range(len(hard_res)):
	OX.append(hard_res[i][0])
	OY.append(hard_res[i][1])
fig=plt.figure()
width=0.35
ind=np.arange(len(OY))
plt.bar(ind,OY,width=width)
plt.xticks(ind+width/2,OX)
fig.autofmt_xdate()
plt.savefig('/home/training/Desktop/hard_winners.png')

#####################################################

# Visualization for Agewise distribution of winners
plt.clf()
aged_panda=aged_panda.set_index('tourney_year')
grouped = aged_panda.groupby(['surface'])
fig,ax=plt.subplots()
for key,group in grouped:
	group['c2'].plot(label=key,ax=ax)
plt.legend(loc='best')
plt.ylabel('Age')
plt.savefig('/home/training/Desktop/age_distrib.png')

################################################

#seed_panda = seed_panda.set_index('winner_seed')
#grouped = seed_panda.groupby(['tourney_level'])
#fig,ax=plt.subplots()
#for key,group in grouped:#
	#group['c2'].plot(label=key,ax=ax)    
#plt.legend(loc='best')
#plt.ylabel('Win Count')
#plt.savefig('/home/training/Desktop/seed_distrib.png')


# Visualization for Grandslam winners seedwise distribution
plt.clf()
OX=[]
OY=[]
perc_OY=[]
sum_OY=0
for i in range(len(seed_distr_op)):
	OX.append(seed_distr_op[i][0])
	OY.append(seed_distr_op[i][1])
for i in range(len(OY)):
    sum_OY=sum_OY+OY[i]
for i in range(len(OY)):
    perc_OY.append(OY[i]*100/sum_OY)
fig=plt.figure()
width=0.35
ind=np.arange(len(perc_OY))
plt.bar(ind,perc_OY,width=width)
plt.xticks(ind+width/2,OX)
fig.autofmt_xdate()
plt.xlabel('Player Seed')
plt.ylabel('Winning %')
plt.savefig('/home/training/Desktop/Grandslam_Winners_Seed_Distr.png')

#####################################################

# Visualization for Match winning rate

plt.clf()
OX=[]
OY=[]
for i in range(len(win_perc_res)):
	OX.append(win_perc_res[i][0])
	OY.append(win_perc_res[i][1])
fig=plt.figure()
width=0.35
ind=np.arange(len(OY))
plt.bar(ind,OY,width=width)
plt.xticks(ind+width/2,OX)
fig.autofmt_xdate()
plt.ylabel('Overall Winning Rate')
plt.savefig('/home/training/Desktop/Winning_Percentage.png')

#####################################################

# Visualization of parameters of Winners and Losers

plt.clf()
fig=plt.figure()
par_comp.plot(x="Parameters",kind='bar')
plt.ylabel('Average Value')
plt.savefig('/home/training/Desktop/Parameters_Comparison.png',bbox_inches='tight',dpi=100)

#####################################################

# Visualization of parameters of top players

plt.clf()
fig=plt.figure()
plyr_comp.plot(x="Parameters",kind='bar')
plt.ylabel('Average Value')
plt.savefig('/home/training/Desktop/Players_Win_Comparison.png',bbox_inches='tight',dpi=100)


############################ END #########################################
