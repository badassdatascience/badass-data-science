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
sys.path.append(application_root_directory + '/badassdatascience/django/infrastructure')
os.environ['DJANGO_SETTINGS_MODULE'] = 'infrastructure.settings'

#
# load Django 
#
import django
django.setup()
