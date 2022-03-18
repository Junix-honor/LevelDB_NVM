#pragma once

#include <string>

#include "leveldb/status.h"

#define KEY_MAX 100
#define VALUE_MAX 100
#define MESSAGE_MAX 100

enum RequestType { PUT = 0, DELETE = 1, GET = 2 };

struct Request {
  RequestType type;
  char key[KEY_MAX];
  char value[VALUE_MAX];
};

struct Respond {
  RequestType type;
  char key[KEY_MAX];
  char value[VALUE_MAX];
  char status[MESSAGE_MAX];
  bool success;
};
