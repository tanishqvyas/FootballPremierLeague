## HELPER FUNCTIONS
from pyspark.sql.functions import col,when
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

# Function to computer the Pass Accuracy
def get_pass_accuracy(num_acc_normal_passes, num_acc_key_passes, num_normal_passes, num_key_passes):

	val = (num_acc_normal_passes+(2*num_acc_key_passes)) / (num_normal_passes + (2*num_key_passes))

	return val


# Function to calculate Duel effectiveness
def get_duel_effectiveness(num_duels_won, num_neutral_duels, total_duels):

	val = (num_duels_won + (0.5*num_neutral_duels)) / total_duels

	return val


# Function to calculate Free Kick effectiveness
def get_freekick_effectiveness(num_effec_free_kicks, num_penalties_scored, total_free_kicks):

	val = (num_effec_free_kicks + num_penalties_scored) / total_free_kicks

	return val


# Function to calculate Shots effectiveness
def get_shots_effectiveness(shots_on_trgt_and_goals, shots_on_trgt_but_not_goals, total_shots):

	val = (shots_on_trgt_and_goals +(0.5*shots_on_trgt_but_not_goals)) / total_shots

	return val


# Function to calculate player contribution
def get_player_contribution(pass_accuracy, duel_effectiveness, free_kick_effectiveness, shots_on_trgt):

	val = (pass_accuracy + duel_effectiveness + free_kick_effectiveness + shots_on_trgt) / 4

	return val


# Function to calculate Player Rating
def get_player_rating(player_performance, existing_player_rating):

	val = (player_performance + existing_player_rating) / 2

	return val


# Function to get the Chances of Winning for A and B
def get_chances_of_winning(strength_of_A, strength_of_B):
	print("-----------------------------Beli chi NAAGin nikal-------------------------------")
	print(strength_of_A, strength_of_B)
	print("----------------------------------Munna vadora ya lagla--------------------------")

	chance_of_A_winning = ((0.5 + strength_of_A) - ( (strength_of_A + strength_of_B)/2 ))*100

	chance_of_B_winning = 100 - chance_of_A_winning

	return chance_of_A_winning, chance_of_B_winning


# Function to Calculate Player Strengths
def get_player_strength(player_rating, list_of_coefficients):

	return player_rating * ( sum(list_of_coefficients) / len(list_of_coefficients))


# Function to calculate Team Strength
def get_team_strength(list_of_player_strength):

	return sum(list_of_player_strength) / 11

# Function to calculate Strengths of two teams
def get_strengths_of_two_teams(Player_RDD, player_chemistry, request):
	
	strength_of_A = 0
	strength_of_B = 0

	player_strength_teamA = []
	player_strength_teamB = []
	#################################################################################
	#CLUSTERING
	requested_player_names=[request["team1"]["player" + str(i)] for i in range(1,12)]+[request["team2"]["player" + str(i)] for i in range(1,12)]
	requested_players_profile=Player_RDD.filter(col("name").isin(requested_player_names))
	profiles=requested_players_profile.collect()
	if len(profiles)!=22:
		print("Players do not exist")
		return None,None
	
	#seeing if there are any players with less than 5 matches and cluster only then
	players_to_approx=requested_players_profile.filter(col("numMatches")<5).collect()
	if len(players_to_approx)!=0:
		#MUST CONVERT EVERY COLUMN TO FLOAT IDK HOW
		
		
		#['name','birthArea','birthDate','foot','role','height','passportArea','weight', 'Id','numFouls','numGoals','numOwnGoals','passAcc','shotsOnTarget','normalPasses','keyPasses','accNormalPasses','accKeyPasses','rating','numMatches']
		vecAssembler = VectorAssembler(inputCols=['height','weight', 'Id','numFouls','numGoals','numOwnGoals','passAcc','shotsOnTarget','normalPasses','keyPasses','accNormalPasses','accKeyPasses','rating','numMatches'], outputCol="features")
		df_kmeans = vecAssembler.transform(Player_RDD)#.select('Id','name','numMatches','rating','features')
		kmeans = KMeans().setK(5).setSeed(1).setFeaturesCol("features")
		model = kmeans.fit(df_kmeans)
		transformed = model.transform(df_kmeans)
		
		avg_rating=transformed.groupBy("prediction").agg({'rating':'avg'})
		#print(avg_rating.schema)	#prediction,avg(rating)
		#print(avg_rating.show())

		temp=transformed.join(avg_rating, transformed.prediction == avg_rating.prediction, 'outer').select(transformed.Id,transformed.name,transformed.numMatches,"avg(rating)")
		players_to_update=temp.filter(col("name").isin(requested_player_names)).filter(col("numMatches")<5).collect()

		for row in players_to_update:
			to_insert=row["avg(rating)"]
			player=row["Id"]

			#Updation ratings in Player profile
			Player_RDD=Player_RDD.withColumn("rating",when(col("Id")==player,to_insert).otherwise(col("rating")))
	
	
	#################################################################################
	
	for player1 in range(1, 12):

		teamA_player_coeff = []
		teamB_player_coeff = []

		teamA_player_rating = Player_RDD.filter(Player_RDD.name == request["team1"]["player" + str(player1)]).select("rating").collect()
		if(len(teamA_player_rating) == 0):
			print("Player ", player1, "of team1 does not exist")
			return None, None
		else:
			teamA_player_rating = teamA_player_rating[0][0]

		teamB_player_rating = Player_RDD.filter(Player_RDD.name == request["team2"]["player" + str(player1)]).select("rating").collect()
		
		if(len(teamB_player_rating) == 0):
			print("Player ", player1, "of team2 does not exist")
			return None, None
			
		else:
			teamB_player_rating = teamB_player_rating[0][0]


		for player2 in range(1, 12):

			# no self loop
			if(player1 == player2):
				continue
			
			# Get player names
			teamA_player1_name = request["team1"]["player" + str(player1)]
			teamA_player2_name = request["team1"]["player" + str(player2)]

			teamB_player1_name = request["team2"]["player" + str(player1)]
			teamB_player2_name = request["team2"]["player" + str(player2)]
			print("-------------------Hermione-", player1 , player2,"----------------------------")
			print("-------------------Hermione-", player1 , player2,"----------------------------")
			print("-------------------Hermione-", player1 , player2,"----------------------------")
			print("-------------------Hermione-", player1 , player2,"----------------------------")
			print("-------------------Hermione-", player1 , player2,"----------------------------")
			print("-------------------Hermione-", player1 , player2,"----------------------------")


			try:
				# Get Player IDs
				teamA_player1_ID = Player_RDD.filter(Player_RDD.name == teamA_player1_name).select("Id").collect()[0][0]
				teamA_player2_ID = Player_RDD.filter(Player_RDD.name == teamA_player2_name).select("Id").collect()[0][0]

				teamB_player1_ID = Player_RDD.filter(Player_RDD.name == teamB_player1_name).select("Id").collect()[0][0]
				teamB_player2_ID = Player_RDD.filter(Player_RDD.name == teamB_player2_name).select("Id").collect()[0][0]

			except:
				print("Invalid Players")
				return None, None

			# Calculate player strengths
			if(teamA_player1_ID < teamA_player2_ID):
				teamA_player_coeff.append(player_chemistry.filter((player_chemistry.player1 == teamA_player1_ID) & (player_chemistry.player2 == teamA_player2_ID)).collect()[0][2])

			else:
				teamA_player_coeff.append(player_chemistry.filter((player_chemistry.player1 == teamA_player2_ID) & (player_chemistry.player2 == teamA_player1_ID)).collect()[0][2])
		
			# For player B
			if(teamB_player1_ID < teamB_player2_ID):
				teamB_player_coeff.append(player_chemistry.filter((player_chemistry.player1 == teamB_player1_ID) & (player_chemistry.player2 == teamB_player2_ID)).collect()[0][2])
			else:

				teamB_player_coeff.append(player_chemistry.filter((player_chemistry.player1 == teamB_player2_ID) & (player_chemistry.player2 == teamB_player1_ID)).collect()[0][2])

		

		# Compute Strengths
		teamA_player_strength = get_player_strength(teamA_player_rating , teamA_player_coeff)
		teamB_player_strength = get_player_strength(teamB_player_rating , teamB_player_coeff)

		player_strength_teamA.append(teamA_player_strength)
		player_strength_teamB.append(teamB_player_strength)

		print("-------------------HARRY---------", player1 ,"--------------------")
		print("-------------------HARRY---------", player1 ,"--------------------")
		print("-------------------HARRY---------", player1 ,"--------------------")
		print("-------------------HARRY---------", player1 ,"--------------------")
		print("-------------------HARRY---------", player1 ,"--------------------")
		print("-------------------HARRY---------", player1 ,"--------------------")
		print("-------------------HARRY---------", player1 ,"--------------------")


	print("-------------------RONALD-----------------------------")
	print("-------------------RONALD-----------------------------")
	print("-------------------RONALD-----------------------------")
	print("-------------------RONALD-----------------------------")
	print("-------------------RONALD-----------------------------")
	print("-------------------RONALD-----------------------------")
	# Get Team Strengths
	strength_of_A = get_team_strength(player_strength_teamA)
	strength_of_B = get_team_strength(player_strength_teamB)

	return strength_of_A, strength_of_B 
