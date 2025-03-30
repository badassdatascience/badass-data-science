#
# load system libraries
#
import sys
import os

#
# user settings
#
application_root_directory = os.environ['BDS_HOME']

#
# set the path and environment
#
sys.path.append(application_root_directory + '/badassdatascience/django')

os.environ['DJANGO_SETTINGS_MODULE'] = 'infrastructure.settings'

#
# in case we need to run this from a Jupyter notebook
#
os.environ['DJANGO_ALLOW_ASYNC_UNSAFE'] = 'true'

#
# load Django 
#
import django
django.setup()
