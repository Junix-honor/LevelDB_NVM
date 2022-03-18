#include <arpa/inet.h>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "leveldb/db.h"

#include "network/network_format.h"
#define QLEN 32
#define BUFSIZE 4096

leveldb::DB* db_;
unsigned short portbase = 0;

void* TCPKeyValueStore(void* arg);
int errexit(const char* format, ...);
int passivesock(const char* service, const char* transport, int qlen);
int passiveTCP(const char* service, int qlen);

void* TCPKeyValueStore(void* arg) {
  int fd = *(int*)arg;
  free(arg);
  Request request;
  leveldb::Status s;
  int cc;

  while (cc = read(fd, &request, sizeof(Request))) {
    if (cc < 0) errexit("read: %s\n", strerror(errno));
    if (request.type == RequestType::GET) {
      std::cout << "查询key:" << request.key << std::endl;
      std::string value;
      s = db_->Get(leveldb::ReadOptions(), request.key, &value);
      if (s.ok())
        std::cout << "查询成功:" << value << std::endl;
      else
        std::cout << "查询失败" << s.ToString() << std::endl;
      Respond respond;
      respond.type = request.type;
      respond.success = s.ok();
      std::string status = s.ToString();
      strncpy(respond.status, status.c_str(), status.length() + 1);
      strncpy(respond.value, value.c_str(), value.length() + 1);
      if (write(fd, &respond, sizeof(Respond)) < 0)
        errexit("write: %s\n", strerror(errno));
      std::cout << "=============================================="
                << std::endl;
    } else if (request.type == RequestType::PUT) {
      std::cout << "添加key:" << request.key << " " << request.value
                << std::endl;
      s = db_->Put(leveldb::WriteOptions(), request.key, request.value);
      if (s.ok())
        std::cout << "添加成功" << std::endl;
      else
        std::cout << "添加失败" << s.ToString() << std::endl;
      Respond respond;
      respond.type = request.type;
      respond.success = s.ok();
      std::string status = s.ToString();
      strncpy(respond.status, status.c_str(), status.length() + 1);
      if (write(fd, &respond, sizeof(Respond)) < 0)
        errexit("write: %s\n", strerror(errno));
      std::cout << "=============================================="
                << std::endl;
    } else if (request.type == RequestType::DELETE) {
      std::cout << "删除key:" << request.key << std::endl;
      s = db_->Delete(leveldb::WriteOptions(), request.key);
      if (s.ok())
        std::cout << "删除成功" << std::endl;
      else
        std::cout << "删除失败" << s.ToString() << std::endl;
      Respond respond;
      respond.type = request.type;
      respond.success = s.ok();
      std::string status = s.ToString();
      strncpy(respond.status, status.c_str(), status.length() + 1);
      if (write(fd, &respond, sizeof(Respond)) < 0)
        errexit("write: %s\n", strerror(errno));
      std::cout << "=============================================="
                << std::endl;
    }
  }
  close(fd);
  return nullptr;
}
int errexit(const char* format, ...) {
  va_list arg;
  va_start(arg, format);
  vfprintf(stderr, format, arg);
  va_end(arg);
  exit(1);
}
int passivesock(const char* service, const char* transport, int qlen)
/*
 * Arguments:
 *      service   - service associated with the desired port
 *      transport - transport protocol to use ("tcp" or "udp")
 *      qlen      - maximum server request queue length
 */
{
  struct servent* pse;    /* pointer to service information entry*/
  struct protoent* ppe;   /* pointer to protocol information entry*/
  struct sockaddr_in sin; /* an Internet endpoint address*/
  int s, type;            /* socket descriptor and socket type*/

  memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = INADDR_ANY;

  /* Map service name to port number */
  if (pse = getservbyname(service, transport))
    sin.sin_port = htons(ntohs((unsigned short)pse->s_port) + portbase);
  else if ((sin.sin_port = htons((unsigned short)atoi(service) + portbase)) ==
           0)
    errexit("can't create passive service %d \n", sin.sin_port);

  /* Map protocol name to protocol number */
  if ((ppe = getprotobyname(transport)) == 0)
    errexit("can't get \"%s\" protocol entry\n", transport);

  /* Use protocol to choose a socket type */
  if (strcmp(transport, "udp") == 0)
    type = SOCK_DGRAM;
  else
    type = SOCK_STREAM;

  /* Allocate a socket */
  s = socket(PF_INET, type, ppe->p_proto);
  if (s < 0) errexit("can't create socket: %s\n", strerror(errno));

  /* Bind the socket */
  if (bind(s, (struct sockaddr*)&sin, sizeof(sin)) < 0)
    errexit("can't bind to %s port: %s\n", service, strerror(errno));
  if (type == SOCK_STREAM && listen(s, qlen) < 0)
    errexit("can't listen on %s port: %s\n", service, strerror(errno));
  return s;
}
int passiveTCP(const char* service, int qlen) {
  return passivesock(service, "tcp", qlen);
}

int main(int argc, char** argv) {
  using namespace leveldb;
  char* service = (char*)"8978";
  struct sockaddr_in fsin;
  unsigned int alen;
  int msock, ssock;
  pthread_t tid;

  std::string dbname_ = "/mnt/d/db_network_test";
  Options options = Options();
  options.create_if_missing = true;
  Status status;
  DB::Open(options, dbname_, &db_);

  switch (argc) {
    case 1:
      break;
    case 2:
      service = argv[1];
      break;
    default:
      errexit("usage: server [port]\n");
  }

  msock = passiveTCP(service, QLEN);
  (void)signal(SIGCHLD, (__sighandler_t)QLEN);

  while (1) {
    alen = sizeof(fsin);
    ssock = accept(msock, (struct sockaddr*)&fsin, &alen);
    if (ssock < 0) {
      if (errno == EINTR) continue;
      errexit("accept: %s\n", strerror(errno));
    }
    int* deliver = (int*)malloc(sizeof(int));
    *deliver = ssock;
    pthread_create(&tid, NULL, TCPKeyValueStore, (void*)deliver);
    pthread_detach(tid);
  }

  close(msock);
  return 0;
}
