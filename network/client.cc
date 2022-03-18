#include <arpa/inet.h>
#include <errno.h>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "network_format.h"

#define LINELEN 500

extern int errno;

int TCPKeyValueStore(const char* host, const char* service);
int errexit(const char* format, ...);
int connectsock(const char* host, const char* service, const char* transport);
int connectTCP(const char* host, const char* service);

int TCPKeyValueStore(const char* host, const char* service) {
  char buf[LINELEN];
  int s, n;
  int outchars, inchars;
  std::cout << "使用方式：" << std::endl;
  std::cout << "增：PUT [KEY] [VALUE]" << std::endl;
  std::cout << "删：DELETE [KEY]" << std::endl;
  std::cout << "查：GET [KEY]" << std::endl;
  std::cout << "==============================================" << std::endl;
  s = connectTCP(host, service);
  while (true) {
    std::cout << ">>";
    std::string type;
    std::cin >> type;
    if (type == "GET") {
      std::string key;
      std::cin >> key;
      Request request;
      request.type = RequestType::GET;
      strncpy(request.key, key.c_str(), key.length() + 1);
      memcpy(buf, &request, sizeof(request));
      (void)write(s, &request, sizeof(Request));
      Respond respond;
      (void)read(s, &respond, sizeof(Respond));
      if (respond.success)
        std::cout << key << ": " << respond.value << std::endl;
      else
        std::cout << respond.status << std::endl;
    } else if (type == "PUT") {
      std::string key, value;
      std::cin >> key >> value;
      Request request;
      request.type = RequestType::PUT;
      strncpy(request.key, key.c_str(), key.length() + 1);
      strncpy(request.value, value.c_str(), value.length() + 1);
      (void)write(s, &request, sizeof(Request));
      Respond respond;
      (void)read(s, &respond, sizeof(Respond));
      if (respond.success)
        std::cout << "PUT SUCCESS" << respond.value << std::endl;
      else
        std::cout << respond.status << std::endl;
    } else if (type == "DELETE") {
      std::string key;
      std::cin >> key;
      Request request;
      request.type = RequestType::DELETE;
      strncpy(request.key, key.c_str(), key.length() + 1);
      (void)write(s, &request, sizeof(Request));
      Respond respond;
      (void)read(s, &respond, sizeof(Respond));
      if (respond.success)
        std::cout << "DELETE SUCCESS" << std::endl;
      else
        std::cout << respond.status << std::endl;
    } else if (type == "QUIT") {
      break;
    } else {
      std::cout << "未知命令" << std::endl;
      std::cout << "=============================================="
                << std::endl;
      std::cout << "使用方式：" << std::endl;
      std::cout << "增：PUT [KEY] [VALUE]" << std::endl;
      std::cout << "删：DELETE [KEY]" << std::endl;
      std::cout << "查：GET [KEY]" << std::endl;
      std::cout << "=============================================="
                << std::endl;
    }
  }
  std::cout << "欢迎您下次继续使用！" << std::endl;
  return 0;
}
int errexit(const char* format, ...) {
  va_list arg;
  va_start(arg, format);
  vfprintf(stderr, format, arg);
  va_end(arg);
  exit(1);
}
int connectsock(const char* host, const char* service, const char* transport)
/*
 * Arguments:
 *      host      - name of host to which connection is desired
 *      service   - service associated with the desired port
 *      transport - name of transport protocol to use ("tcp" or "udp")
 */
{
  struct hostent* phe;    /* pointer to host information entry    */
  struct servent* pse;    /* pointer to service information entry */
  struct protoent* ppe;   /* pointer to protocol information entry*/
  struct sockaddr_in sin; /* an Internet endpoint address     */
  int s, type;            /* socket descriptor and socket type    */

  memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;

  /* Map service name to port number */
  if (pse = getservbyname(service, transport))
    sin.sin_port = pse->s_port;
  else if ((sin.sin_port = htons((unsigned short)atoi(service))) == 0)
    errexit("can't get \"%s\" service entry\n", service);

  /* Map host name to IP address, allowing for dotted decimal */
  if (phe = gethostbyname(host))
    memcpy(&sin.sin_addr, phe->h_addr, phe->h_length);
  else if ((sin.sin_addr.s_addr = inet_addr(host)) == INADDR_NONE)
    errexit("can't get \"%s\" host entry\n", host);

  /* Map transport protocol name to protocol number */
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

  /* Connect the socket */
  if (connect(s, (struct sockaddr*)&sin, sizeof(sin)) < 0)
    errexit("can't connect to %s.%s: %s\n", host, service, strerror(errno));
  return s;
}
int connectTCP(const char* host, const char* service) {
  return connectsock(host, service, "tcp");
}

int main(int argc, char* argv[]) {
  char* host = (char*)"localhost";
  char* service = (char*)"8978";
  switch (argc) {
    case 1:
      host = (char*)"localhost";
      break;
    case 3:
      service = argv[2];
    case 2:
      host = argv[1];
      break;
    default:
      fprintf(stderr, "usage:client [host[port]]\n");
      exit(1);
  }
  std::cout << "欢迎使用key/value网络存取服务" << std::endl;
  std::cout << "==============================================" << std::endl;
  TCPKeyValueStore(host, service);
  exit(0);
}