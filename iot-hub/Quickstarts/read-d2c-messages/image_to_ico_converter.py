from PIL import Image 
filename = r'C:\Users\jabarrantes\Documents\GitHub\azure-iot-samples-python\iot-hub\Quickstarts\read-d2c-messages\crop-logo.png'
img = Image.open(filename)
img.save('logo.ico')