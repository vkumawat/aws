#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
#
# Developer : Kumawat, Virendra <virendrakumawat86@gmail.com>
#
# SYNOPSIS:
# ---------
#   This script will list the buckets and delete the older contents.
#
# DESCRIPTION:
# -----------
#   This script will list the buckets and delete the older contents.
#
# EXAMPLES:
# ---------
#   python3.9 clean_bucket_contents.py  
#
# NOTES:
# ------
#   python 3.x should be installed
#   python boto3 package should be installed
"""

import os
import sys
import time
import boto3
import datetime
import botocore

class Delete_Contents:

  def __init__( self ):
    pass

  def clean_s3_bucket_contents( self ):
    try:
      message = "Clean S3 buckets contents older than 30 days..."
      print( message )
      day_after_days_difference = datetime.date.today() - datetime.timedelta(30)
      message = "Day after number of days difference is [" 
      message+= str( day_after_days_difference ) + "]."
      print( message )
      day_after_days_difference_var = day_after_days_difference.strftime("%Y-%m-%d")
      diff = day_after_days_difference_var.split("-")
      # S3 client list of buckets with name and is creation date
      s3_resource = boto3.resource('s3', aws_access_key_id = '<aws access key>', aws_secret_access_key = '<aws secret key>')
      buckets = s3_resource.buckets.all()
      for bucket in buckets:
        message = "Current s3 bucket is [" + str( bucket.name ) + "]."
        print( message )
        buckets_obj = s3_resource.Bucket( bucket.name )
        try:
          for obj in buckets_obj.objects.all():
            objtime= obj.last_modified.replace(tzinfo = None).strftime("%Y,%m,%d").split(",")
            if datetime.datetime( int( objtime[0] ), int( objtime[1] ), int( objtime[2]) )  < datetime.datetime( int( diff[0] ), int( diff[1] ), int( diff[2] )):
              message = "Deleting Object [" + str( obj ) + "]."
              print( message )
              if obj.delete():
                message = "Successfully deleted object [" + str( obj ) + "]."
                print( message )
              else:  
                message = "Failed to delete the object [" + str( obj ) + "]."
                print( message )
        except botocore.exceptions.ClientError as err:
            message = str( err ) + " for bucket [" + bucket.name + "]."
            print( message )
            pass
    except:
      raise

if __name__ == "__main__":

  delete_contents = None

  try:
    # Create object of delete_contents class.
    delete_contents = Delete_Contents()
    delete_contents.clean_s3_bucket_contents()
  except:
    raise
  finally:
    del delete_contents
