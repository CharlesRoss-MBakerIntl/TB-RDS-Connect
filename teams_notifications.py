import requests
import json


#Report Errors to Teams Notifications Channel
def teams_notification(channel, type, summary, message):

    """
    Reports updates in the lambda processing script for the tidal basin projects to a chosen teams channel.  
    """

    error = None

    #Set Message Stats Based on Type
    if type == "Error":
         color = "#C21807"
         title = "ERROR"

    elif type == "Success":
         color = "#52a447"
         title = "SUCCESS"


    #Send to Teams Channel
    try:
        
        payload = {
            '@type': 'MessageCard',
            '@context': 'http://schema.org/extensions',
            'themeColor': color,
            'summary': summary,
            'sections': [
                {
                    'activityTitle': title,
                    'text': message
                }
            ]
        }


        #Set Payload Headers
        headers = {
            'Content-Type': 'application/json'
        }


        #Submit Message
        response = requests.post(channel, data=json.dumps(payload), headers=headers)


        #If Request Connection Failed, Create Event and Raise Error
        if response.status_code != 200:
                error = f"Failed to submit error notification to {channel}"
        
    #Return Error if Failed
    except Exception as e:
        error = f"Failed to submit error notification to {channel}"


    return error





