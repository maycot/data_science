# -*- coding: utf-8 -*

"""
OCR model
"""

from PIL import Image
import pytesseract
import cv2
import os

pytesseract.pytesseract.tesseract_cmd = 'C:\\Program Files (x86)\\Tesseract-OCR\\tesseract.exe'


def main(data_dir):

    # Get the filenames and labels from the train set
    train_filenames = [os.path.join(data_dir, f) 
                       for f in os.listdir(data_dir)
                       if f.endswith('.jpeg')]
    
    # load the example image and convert it to grayscale
    image = cv2.imread(train_filenames[10])
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    # OCR and print result (one example)
    text = pytesseract.image_to_string(gray)
    print("*** Image analysée : {} ***\n".format(train_filenames[10]))
    print(text)
    # show the output images
    #cv2.imshow("Image", gray)
    #cv2.waitKey(5)
    
if __name__ == '__main__':
    data_dir = './data/train'
    main(data_dir)
