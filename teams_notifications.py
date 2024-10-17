import requests
import json


#Report Errors to Teams Notifications Channel
def channel_notification(alert_type, summary, message, error = None):

    """
    This function sends notifications to a Teams channel based on the provided parameters.

    Parameters:
    - alert_type: Type of notification (Error or Success).
    - summary: A brief summary of the notification.
    - message: Detailed message content of the notification.
    - error: Optional parameter to handle any errors encountered.

    Explanation:
    This function constructs a notification payload based on the alert type (Error or Success) and sends it to a Teams channel. It sets the color and title based on the alert type, then creates a JSON payload containing the summary and message. The payload is sent to the specified channel using an HTTP POST request. If the request is unsuccessful (status code other than 200), it logs an error message. In case of any exceptions during the process, it also handles and logs an error message.

    Return:
    The function returns an error message if there was a failure during the notification submission.
    """

    #Set Teams Channel for Notifications
    channel = ""

    #Set Error Catch
    error = None

    #Set Message Stats Based on Type
    if alert_type == "Error":
         color = "#C21807"
         title = "ERROR"

    elif alert_type == "Success":
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





