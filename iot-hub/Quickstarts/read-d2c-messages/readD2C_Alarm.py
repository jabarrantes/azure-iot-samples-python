
from azure.eventhub import TransportType
from azure.eventhub import EventHubConsumerClient
import json
from beepy import beep
import os 

# If you have access to the Event Hub-compatible connection string from the Azure portal, then
# you can skip the Azure CLI commands above, and assign the connection string directly here.
CONNECTION_STR = f'Endpoint=sb://ihsuprodsnres017dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=9TuRqRLsoBgLi7RpGWafgvjCexduBrM9qBXJ0ttvmQI=;EntityPath=iothub-ehub-iothubfrut-3381630-6ba88323eb'
TIMER = 0
VALUE_HISTORY = []
def message_process(message):
    msg = json.loads(message)
    print("Molino valueß", msg['Molino'])
    global TIMER 
    global VALUE_HISTORY
    if msg['Molino'] > 10 and msg['Molino'] < 200:  
        TIMER += 1 
        VALUE_HISTORY.append(msg['Molino'])
        print('Una medicion en vacio !- -- - - - \n')
        if TIMER > 0: 
            noise_Generate()
    else: 
        TIMER = 0
        VALUE_HISTORY.clear()
def noise_Generate():
    #make noise
    print('Molino lleva mucho tiempo en Vacio ------------------\n')
    print('Ultimas 5 mediciones\n')
    beep(sound=3)
    for i in VALUE_HISTORY:
        print("Medicion ", i)
        print()

# Define callbacks to process events
def on_event_batch(partition_context, events):
    for event in events:
        print("Telemetry received: ", event.body_as_str())
        message_process(event.body_as_str())
    partition_context.update_checkpoint()

def on_error(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        print("An exception: {} occurred during receiving from Partition: {}.".format(
            partition_context.partition_id,
            error
        ))
    else:
        print("An exception: {} occurred during the load balance process.".format(error))


def main():
    os.system("beep -f 555 -l 460")
    client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group="iotfrutilight",
    )
    try:
        with client:
            client.receive_batch(
                on_event_batch=on_event_batch,
                on_error=on_error
            )
    except KeyboardInterrupt:
        print("Receiving has stopped.")

if __name__ == '__main__':
    main()
