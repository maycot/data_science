# -*- coding: utf-8 -*-

"""
Split original pdf file into multiple images
Each page in the pdf file is converted into an image
"""

import os
from PyPDF2 import PdfFileReader, PdfFileWriter
from wand.image import Image


def split_pdf_pages(pdf_dir, one_page_pdf_dir):

    if not os.path.exists(one_page_pdf_dir):
        os.makedirs(one_page_pdf_dir)
    
    pdf_filenames = [filename for filename in os.listdir(pdf_dir)
                     if filename.endswith('.pdf')]
    
    for pdf_filename in pdf_filenames:

        with open(os.path.join(pdf_dir, pdf_filename),
                  "rb") as input_stream:
            input_pdf = PdfFileReader(input_stream)

            if input_pdf.flattenedPages is None:
                # flatten the file using getNumPages()
                input_pdf.getNumPages()

            for num_page, page in enumerate(input_pdf.flattenedPages):
                output = PdfFileWriter()
                output.addPage(page)
                
                filename = os.path.join(
                    one_page_pdf_dir, u"{pdf_filename}_{num_page:04d}.pdf".format(
                        pdf_filename=pdf_filename[:-4], num_page=num_page))
                
                with open(filename, "wb") as output_stream:
                    output.write(output_stream)


def convert_one_page_pdf_to_image(one_page_pdf_dir, images_dir):

    if not os.path.exists(images_dir):
        os.makedirs(images_dir)
    
    pdf_filenames = [filename for filename in os.listdir(one_page_pdf_dir)
                     if filename.endswith('.pdf')]

    for pdf_filename in pdf_filenames: 
        with Image(filename=os.path.join(one_page_pdf_dir, pdf_filename)) as img:
            with img.convert('jpeg') as converted:
                converted.save(filename=os.path.join(images_dir, 
                               '{}.jpeg'.format(pdf_filename[:-4])))
                

if __name__ == '__main__':
    
    pdf_dir = './pdf'
    one_page_pdf_dir = './one_page_pdf'
    images_dir = './images'
    split_pdf_pages(pdf_dir, one_page_pdf_dir)
    convert_one_page_pdf_to_image(one_page_pdf_dir, images_dir)