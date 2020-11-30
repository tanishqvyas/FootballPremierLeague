from socket import *
from utils.helper import *
import sys
import json
import time
import os

# Function to handle request 1
def handle_request_one(request, Metrics_RDD, Player_RDD):
    
    # Invalid request handler variables
    msg = "Invalid Request"
    valid_request = True

    player_counter = {
        "team1" : {
            "GK" : 0,
            "DF" : 0,
            "MD" : 0,
            "FW" : 0
        },

        "team2" : {
            "GK" : 0,
            "DF" : 0,
            "MD" : 0,
            "FW" : 0
        }
    }

    #--------------------------Initializing the response dict-------------------------
    response = dict()
    response["team1"] = dict()
    response["team2"] = dict()
    response["team1"]["name"] = "Name 1"
    response["team2"]["name"] = "Name 2"
    response["team1"]["winning chance"] = 0
    response["team2"]["winning chance"] = 0


    #--------------------------Process the request------------------------------------

    # Finding counts of players in different role's
    for i in range(1, 12):

        # Fetching the name
        team1_cur_player_name = request["team1"]["player"+str(i)]
        team2_cur_player_name = request["team2"]["player"+str(i)]

        # Fetching Player  data
        team1_cur_player_data = Player_RDD.filter(Player_RDD.name == team1_cur_player_name).collect()
        team2_cur_player_data = Player_RDD.filter(Player_RDD.name == team2_cur_player_name).collect()


        if(len(team1_cur_player_data) == 0):
            valid_request = False
            msg = "Player : " + team1_cur_player_name + " does not exist"
            break
        else:
            # Fetch the Role of the player
            current_player_pos = team1_cur_player_data[0][4]

            player_counter["team1"][current_player_pos] += 1
        

        if(len(team2_cur_player_data) == 0):
            valid_request = False
            msg = "Player : " + team2_cur_player_name + " does not exist"
            break
        else:
            # Fetch the Role of the player
            current_player_pos = team1_cur_player_data[0][4]

            player_counter["team2"][current_player_pos] += 1


    # check For team validity if all players exist
    if(valid_request): 

        # Invalid Team 1
        if not (player_counter["team1"]["GK"] == 1 and player_counter["team1"]["DF"] >= 3 and player_counter["team1"]["MD"] >= 2 and player_counter["team1"]["FW"] >= 1):
            
            valid_request = False
            msg = "Team 1 is Invalid"
        
        # Invalid Team 2
        if not (player_counter["team2"]["GK"] == 1 and player_counter["team2"]["DF"] >= 3 and player_counter["team2"]["MD"] >= 2 and player_counter["team2"]["FW"] >= 1):
            
            valid_request = False
            msg = "Team 2 is Invalid"

    
    # --------------------------- Prepare the Response ------------------------
    response["team1"]["name"] = request["team1"]["name"]
    response["team2"]["name"] = request["team2"]["name"]

    # Compute Strengths of Each Team
    # strength_of_team1, strength_of_team2 = 

    # Fetch Winning Chances for Teams
    # response["team1"]["winning chance"], response["team2"]["winning chance"] = get_chances_of_winning(strength_of_team1, strength_of_team2)
    

    #-----------------------------Returning the response----------------------------------
    if valid_request:
        return response
    else:
        response = dict()
        response["msg"] = msg
        return response


# Function to handle request 2
def handle_request_two(request, Metrics_RDD, Player_RDD):
    
    # Invalid request handler variables
    msg = "Invalid Request"
    valid_request = True

    #--------------------------Initializing the response dict-------------------------
    response = dict()

    fields_list = ["name", "birthArea", "birthDate", "foot", "role", "height", "passportArea", "weight", "fouls", "goals", "own_goals", "percent_pass_accuracy", "percent_shots_on_target"]
    for field in fields_list:
        response[field] = "sample data"


    #--------------------------Process the request------------------------------------
    requested_player_name = request["name"]
    matching_player_row = Player_RDD.filter(Player_RDD.name == requested_player_name).collect()

    # If no player found
    if(len(matching_player_row) == 0):
        msg = "Player Not Found"
        valid_request = False

    else:
        # Converting RDD to json
        response["name"] = matching_player_row[0][0]
        response["birthArea"] = matching_player_row[0][1]

        response["birthDate"] = str(matching_player_row[0][2].year) + "-" + str(matching_player_row[0][2].month) + "-" + str(matching_player_row[0][2].day)
        
        response["foot"] = matching_player_row[0][3]
        response["role"] = matching_player_row[0][4]
        response["height"] = matching_player_row[0][5]
        response["passportArea"] = matching_player_row[0][6]
        response["weight"] = matching_player_row[0][7]
        response["fouls"] = matching_player_row[0][9]
        response["goals"] = matching_player_row[0][10]
        response["own_goals"] = matching_player_row[0][11]
        response["percent_pass_accuracy"] = matching_player_row[0][12]
        response["percent_shots_on_target"] = matching_player_row[0][13]


    #-----------------------------Returning the response----------------------------------
    if valid_request:
        return response
    else:
        response = dict()
        response["msg"] = msg
        return response


# Function to handle request 3
def handle_request_three(request, Metrics_RDD, Player_RDD):
    
    # Invalid request handler variables
    msg = "Invalid Request"
    valid_request = True

    #--------------------------Initializing the response dict-------------------------
    response = dict()
    response["yellow_cards"] = []
    response["red_cards"] = []

    # String Fields and Number fields
    fields_list = ["date", "duration", "winner", "venue", "gameweek"]
    for field in fields_list:
        response[field] = "sample data"

    # List of dictionaries
    response["goals"] = []
    response["own_goals"] = []


    #--------------------------Process the request------------------------------------
    response["date"] = request["date"]



    #-----------------------------Returning the response----------------------------------
    if valid_request:
        return response
    else:
        response = dict()
        response["msg"] = msg
        return response



# Request Handler
def request_handler(req_type, request, Metrics_RDD, Player_RDD):
    
    if(req_type == 1):
        return handle_request_one(request, Metrics_RDD, Player_RDD)
    
    elif (req_type == 2):
        return handle_request_two(request, Metrics_RDD, Player_RDD)
    
    elif (req_type == 3):
        return handle_request_three(request, Metrics_RDD, Player_RDD)
    
    else:
        response = dict()
        response["msg"] = "Invalid request"
        return response



# User Interface Handler
def start_user_service(Metrics_RDD, Player_RDD):


    # File paths for Request and Reponse Files
    request_file_path = os.path.join("request_response_data", "request.txt")
    response_file_path = os.path.join("request_response_data", "response.txt")

    # Opening  the request File and response file
    request_file = open(request_file_path, "r")
    response_file = open(response_file_path, "w+")

    # Loop to read Json Requests from user
    # And to write the corresponding response to response file
    while True:

        # --------------------- Reading Response ---------------------------------
        # Get the Json request from the file
        request = request_file.readline()
        print("Sending : ", request)

        # If not line : EOF
        if not request:
            print("EOF for request.txt")
            break

        # converting request json to dict
        request = json.loads(request)


        # --------------------- Processing Response ------------------------------
        # Process the request
        test_response = request_handler(request["req_type"], request, Metrics_RDD, Player_RDD)


        # --------------------- Writing Response ---------------------------------
        # Converting the response dict to Json format str to write to file
        test_response = json.dumps(test_response)
        test_response = str(test_response)

        # Write the response to the response file
        response_file.write(test_response)
        response_file.write("\n")

        time.sleep(1)


    # Closing the files
    request_file.close()
    response_file.close()

