from socket import *
import sys
import json
import time
import os

# Function to handle request 1
def handle_request_one(request):
    
    # Invalid request handler variables
    msg = "Invalid Request"
    valid_request = True


    # Initializing the response dict
    response = dict()
    response["team1"] = dict()
    response["team2"] = dict()
    response["team1"]["name"] = "Name 1"
    response["team2"]["name"] = "Name 2"
    response["team1"]["winning chance"] = 0
    response["team2"]["winning chance"] = 0

    # Process the request



    # Returning the response
    if valid_request:
        return response
    else:
        response = dict()
        response["msg"] = msg
        return response


# Function to handle request 2
def handle_request_two(request):
    
    # Invalid request handler variables
    msg = "Invalid Request"
    valid_request = True

    # Initializing the response dict
    response = dict()

    fields_list = ["name", "birthArea", "birthDate", "foot", "role", "height", "passportArea", "weight", "fouls", "goals", "own_goals", "percent_pass_accuracy", "percent_shots_on_target"]
    for field in fields_list:
        response[field] = "sample data"


    # Process the request



    # Returning the response
    if valid_request:
        return response
    else:
        response = dict()
        response["msg"] = msg
        return response


# Function to handle request 3
def handle_request_three(request):
    
    # Invalid request handler variables
    msg = "Invalid Request"
    valid_request = True

    # Initializing the response dict
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


    # Process the request



    # Returning the response
    if valid_request:
        return response
    else:
        response = dict()
        response["msg"] = msg
        return response



# Request Handler
def request_handler(req_type, request):
    
    if(req_type == 1):
        return handle_request_one(request)
    
    elif (req_type == 2):
        return handle_request_two(request)
    
    elif (req_type == 3):
        return handle_request_three(request)
    
    else:
        response = dict()
        response["msg"] = "Invalid request"
        return response



# User Interface Handler
def start_user_service():

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
        test_response = request_handler(request["req_type"], request)


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



if __name__ == "__main__":

    # Start the CLI for user
    start_user_service()
