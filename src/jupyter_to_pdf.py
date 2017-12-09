import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert import PDFExporter
import os

# loading from local model(s)
# from file_handler import files_handler

#----------------------------------------------------------
def pdf_convertor(in_name, out_name, current_dir, new_dir):
    #notebook_filename = "telstra-exploring-data.ipynb"
    with open(in_name+".ipynb") as f:
        print 'jupyter notebook is taken from dir:', current_dir
        nb = nbformat.read(f, as_version=4)
        
    ep = ExecutePreprocessor(timeout=600, kernel_name='python2') #python3
    ep.preprocess(nb, {'metadata': {'path': new_dir}})
    pdf_exporter = PDFExporter()
    pdf_data, resources = pdf_exporter.from_notebook_node(nb)

    with open(out_name+".pdf", "wb") as f:
        print 'pdf report is saved in dir:', new_dir
        f.write(pdf_data)
        f.close()
        
def jupyter_convertor(in_name, out_name, current_dir, new_dir):
    #notebook_filename = "telstra-exploring-data.ipynb"
    
    if not os.path.exists(new_dir):
        print 'Creating directory for reports ...'
        os.makedirs(new_dir)
        print new_dir
        pdf_convertor(in_name, out_name, current_dir, new_dir)     
    else: 
        print 'Directory exists for reports at ', new_dir
        pdf_convertor(in_name, out_name, current_dir, new_dir) 
