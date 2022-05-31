#include <cstdio>
#include <subprocess.hpp>
namespace sp = subprocess;
using sp::PIPE;

#include <zstd.h>

int main(int argc, char* argv[]) {
  printf("START!\n");

  char buf[1024], buf2[1024];
  FILE *fd {fopen(argv[0], "rb")};
  fread(buf, sizeof(buf), 1, fd);
  fclose(fd);

  const decltype(sizeof(0)) sz = ZSTD_compress(buf2, sizeof(buf2), buf, sizeof(buf), 9);

  auto procXxd = sp::Popen({"xxd"}, sp::input{PIPE}, sp::output{PIPE});
  printf("compressed size: %d\n", sz);
  procXxd.send(buf2, sz);
  procXxd.close_input();
  auto procSed = sp::Popen({"sed", "s/0/X/g"}, sp::input{procXxd.output()}, sp::output{PIPE});
  auto procHead = sp::Popen({"head", "-n", "10"}, sp::input{procSed.output()}, sp::output{PIPE});
  auto res = procHead.communicate().first;
  printf("%s\n", res.buf.data());
  printf("ret head: %d\n", procHead.wait());
  printf("ret sed: %d\n", procSed.wait());
  printf("ret xxd: %d\n", procXxd.wait());

  printf("FINISH!\n");
  return 0;
}
