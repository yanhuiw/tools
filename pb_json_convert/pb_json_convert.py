#!/usr/bin/python
#coding: utf-8

import os
import sys
import json
import struct
import snappy
from google.protobuf import json_format
# test pb
import test_pb2

# operator type
kOperatePbToJson = 'pb_to_json'
kOperateJsonToPb = 'json_to_pb'
kOperateTypeList = [kOperatePbToJson, kOperateJsonToPb]

# compress type
kCompressTypeDefault = 0
kCompressTypeNotCompress = 1  # no compress
kCompressTypeSnappyCompress = 2  # snappy compress

# proto type
class pbType:
  def __init__(self, name, pb_func,
      pb_to_json_header_func, json_to_pb_header_func,
      compress_type, compress_func, uncompress_func,
      pb_to_json_func, json_to_pb_func):
    self.name = name
    self.pb_func = pb_func
    # pb to json for header
    self.pb_to_json_header_func = pb_to_json_header_func
    # json to pb for header
    self.json_to_pb_header_func = json_to_pb_header_func
    # compress type for main body
    self.compress_type = compress_type
    # compress function for main body
    self.compress_func = compress_func
    # uncompress function for main body
    self.uncompress_func = uncompress_func
    # pb to json function for main body
    self.pb_to_json_func = pb_to_json_func
    # json to pb function for main body
    self.json_to_pb_func = json_to_pb_func

def compress_func(compress_type, record_str):
  if compress_type == kCompressTypeSnappyCompress:
    record_str = snappy.uncompress(record_str)
  return record_str

def uncompress_func(compress_type, record_str):
  if compress_type == kCompressTypeSnappyCompress:
    record_str = snappy.compress(record_str)
  return record_str

def pb_to_json(pb_type, compress_type, pb_file, json_file):
  record_size_str = pb_file.read(4)
  if not record_size_str:
    return False
  record_size, = struct.unpack('i', record_size_str)
  record_str = pb_file.read(record_size)
  record_str = pb_type.uncompress_func(compress_type, record_str)
  record = pb_type.pb_func()
  record.ParseFromString(record_str)
  record_dict = json_format.MessageToDict(record, preserving_proto_field_name=True)
  record_json = json.dumps(record_dict)
  json_file.write(record_json.encode('utf-8'))
  json_file.write('\n')
  return True

def json_to_pb(pb_type, compress_type, json_file, pb_file):
  record_json = json_file.readline()
  if len(record_json) == 0:
    return False
  record_json = record_json.strip()
  if len(record_json) == 0:
    return True
  record = pb_type.pb_func()
  json_format.Parse(record_json, record)
  record_str = pb_type.compress_func(compress_type, record_str)
  record_size = len(record_str)
  record_size_str = struct.pack('i', record_size)
  pb_file.write(record_size_str)
  pb_file.write(record_str)
  return True

def pb_to_json_header_func_none(pb_type, pb_file, json_file):
  return True

def pb_to_json_header_func(pb_type, pb_file, json_file):
  return pb_to_json(pb_type, kCompressTypeDefault, pb_file, json_file)

def json_to_pb_header_func_none(pb_type, json_file, pb_file):
  return True

def json_to_pb_header_func(pb_type, json_file, pb_file):
  return json_to_pb(pb_type, kCompressTypeDefault, json_file, pb_file)

def pb_to_json_func(pb_type, pb_file_path, json_file_path):
  pb_file = None
  json_file = None
  try:
    pb_file = open(pb_file_path, 'rb')
    json_file = open(json_file_path, 'wb')

    if not pb_type.pb_to_json_header_func(pb_type, pb_file, json_file):
      return False

    while True:
      if not pb_type.pb_to_json(pb_type, pb_type.compress_type, pb_file, json_file):
        break
  
  except Exception as e:
    print(str(e))
    return False
  finally:
    result = True
    for (file, file_path) in ((pb_file, pb_file_path), (json_file, json_file_path)):
      if file:
        try:
          file.close()
        except Exception as e:
          print(str(e))
          result = False
    return result

def json_to_pb_func(pb_type, json_file_path, pb_file_path):
  json_file = None
  pb_file = None
  try:
    json_file = open(json_file_path, 'rb')
    pb_file = open(pb_file_path, 'wb')

    if not pb_type.json_to_pb_header_func(pb_type, json_file, pb_file):
      return False
    
    while True:
      if not pb_type.json_to_pb(pb_type, pb_type.compress_type, json_file, pb_file):
        break
  except Exception as e:
    print(str(e))
    return False
  finally:
    result = True
    for (file, file_path) in ((pb_file, pb_file_path), (json_file, json_file_path)):
      if file:
        try:
          file.close()
        except Exception as e:
          print(str(e))
          result = False
    return result

def convert_func(pb_type, operate_type, input_file_path, output_file_path):
  if operate_type == kOperatePbToJson:
    return pb_type.pb_to_json_func(pb_type, input_file_path, output_file_path)
  elif operate_type == kOperateJsonToPb:
    return pb_type.json_to_pb_func(pb_type, input_file_path, output_file_path)
  else:
    return False

# test pb
kPbTypeTest = PbType('test', Test,
    pb_to_json_header_func_none, json_to_pb_header_func_none,
    kCompressTypeDefault, compress_func, uncompress_func,
    pb_to_json_func, json_to_pb_func)
kPbTypeOrderedDict[kPbTypeTest.name] = kPbTypeTest

read_me = """manual:
python pb_json_convert.py pb_type operate_type input_file_path output_file_path
examples:
python pb_json_convert.py test pb_to_json test.pb test.json
python pb_json_convert.py test json_to_pb test.json test.pb"""

if __name__ == '__main__':
  if len(sys.argv) != 5:
    print(read_me)

  pb_type_name = sys.argv[1]
  operate_type = sys.argv[2]
  input_file_path = sys.argv[3]
  output_file_path = sys.argv[4]
  pb_type = kPbTypeOrderedDict[pb_type_name]
  if convert_func(pb_type, operate_type, input_file_path, output_file_path):
    sys.exit(0)
  
  sys.exit(1)
  

  

