## HELPER FUNCTIONS

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

	chance_of_A_winning = ((0.5 + strength_of_A) - ( (strength_of_A + strength_of_B)/2 ))*100

	chance_of_B_winning = 100 - chance_of_A_winning

	return chance_of_A_winning, chance_of_B_winning


# Function to Calculate Player Strengths
def get_player_strength(player_rating, list_of_coefficients):

	return player_rating * ( sum(list_of_coefficients) / len(list_of_coefficients))


# Function to calculate Team Strength
def get_team_strength():

	cur_strength = 0

	# Find average of all team players
	for player in range(11):
		pass

		# cur_strength += get_player_strength(player)

	return cur_strength / 11

# Function to calculate Strengths of two teams
def get_strengths_of_two_teams():

	pass 