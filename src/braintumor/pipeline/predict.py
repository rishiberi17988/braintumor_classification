import numpy as np
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image
import os



class PredictionPipeline:
    def __init__(self,filename):
        self.filename =filename


    
    def predict(self):
        # load model
        model = load_model(os.path.join("artifacts","training", "model.h5"))

        imagename = self.filename
        img = image.load_img(imagename, target_size=(224, 224))
        img_array = image.img_to_array(img)
        img_array = np.expand_dims(img_array, axis=0)
        img_array /= 255.0

        prediction = model.predict(img_array)
        predicted_class = np.argmax(prediction[0])
        print(predicted_class)

        if predicted_class == 0:
            prediction = 'glioma'
            return [{ "image" : prediction}]
        elif predicted_class == 1:
            prediction = 'meningioma'
            return [{ "image" : prediction}]
        elif predicted_class == 2:
            prediction = 'notumor'
            return [{ "image" : prediction}]
        else :
            prediction = 'pituitarty'
            return [{ "image" : prediction}]
    
    