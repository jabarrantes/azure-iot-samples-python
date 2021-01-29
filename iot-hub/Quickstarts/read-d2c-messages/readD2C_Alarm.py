
from azure.eventhub import TransportType
from azure.eventhub import EventHubConsumerClient
import json
import os 
import winsound 
import time as t
import threading
import concurrent.futures
from playsound import playsound
from win10toast import ToastNotifier
# If you have access to the Event Hub-compatible connection string from the Azure portal, then
# you can skip the Azure CLI commands above, and assign the connection string directly here.
CONNECTION_STR = f'Endpoint=sb://ihsuprodsnres017dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=9TuRqRLsoBgLi7RpGWafgvjCexduBrM9qBXJ0ttvmQI=;EntityPath=iothub-ehub-iothubfrut-3381630-6ba88323eb'


VALUE_HISTORY = [] #List that contains history of values for last few measurements
ALARM_FLAG = False
ALARM_ENABLE = True 
#Notification 
toaster = ToastNotifier()
#Declaring Lock 
lock = threading.Lock()

def message_process(message):
    msg = json.loads(message)
    global ALARM_ENABLE
    global ALARM_FLAG
    print("Molino value", msg['Molino'])
    VALUE_HISTORY.append(float(msg['Molino']))
    if(msg['test'] == '20 Min en Vacio'): 
        if(ALARM_ENABLE):
            ALARM_FLAG = True
    if(msg['Molino'] < 5 or msg['Molino'] > 50):
        print("Enabling Alarm Again ---")
        lock.acquire()
        ALARM_ENABLE = True
        lock.release()
def trigger_Alarm():
    while True: 
        while ALARM_FLAG:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                executor.submit(create_notif)
                executor.submit(generate_sound)
            #make noise
            #winsound.Beep(2000,8000)
            print('Molino lleva mucho tiempo en Vacio ------------------\n')
            print('Ultimas 5 mediciones\n')
            for i in VALUE_HISTORY:
                print("Medicion ", i)
                print()
        t.sleep(0.1)

def generate_sound():
    for i in range(3):
        #winsound.PlaySound('alarm.wav', winsound.SND_FILENAME)
        if ALARM_FLAG:
            #playsound(r'C:\Users\jabarrantes\Documents\GitHub\azure-iot-samples-python\iot-hub\Quickstarts\read-d2c-messages\alarm.wav')
            winsound.PlaySound(r'C:\Users\jabarrantes\Documents\GitHub\azure-iot-samples-python\iot-hub\Quickstarts\read-d2c-messages\alarm.wav', winsound.SND_LOOP | winsound.SND_ASYNC)
        else:
            winsound.PlaySound(None, winsound.SND_PURGE)
            break
        t.sleep(0.3)
def disable_alarm():
    global ALARM_FLAG
    global ALARM_ENABLE
    print("Disabling Alarms")
    lock.acquire()
    ALARM_FLAG = False 
    ALARM_ENABLE = False 
    lock.release()
    winsound.PlaySound(None, winsound.SND_PURGE)
    print("Alarm off")     

def create_notif():
    for i in range(2):
        if ALARM_FLAG:
            toaster.show_toast("MOLINO LLEVA TIEMPO EN VACIO",
                            "REVISAR FUNCIONAMIENTO DEL MOLINO",
                            icon_path=r'C:\Users\jabarrantes\Documents\GitHub\azure-iot-samples-python\iot-hub\Quickstarts\read-d2c-messages\logo.ico',
                            duration=5, callback_on_click=disable_alarm)
        else:
            break
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
    #main()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(main)
        executor.submit(trigger_Alarm)
