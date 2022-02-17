
import subprocess as sub

get_contents = sub.call(['java', '-jar', "/ES/DocumentsTextExtract-import-1.0.jar", '/ES/ADSP_Summary.pdf'])
print('\n\n')
print(get_contents)