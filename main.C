#include <stdio.h>

#include "concurrent_revisions.h"

int main() {
  Versioned<int> x(0), y(0);
  class action : public IAction {
    Versioned<int> &x, &y;
    void Do() {
      if (0 == x.Get()) y.Set(1);
    }
  public:
    action(Versioned<int> &x, Versioned<int> &y) : x(x), y(y) {}
  } a(x, y);

  Revision *const r = Revision::currentRevision()->Fork(&a);
  if (0 == y.Get()) x.Set(1);

  printf("before join: %d, %d\n", x.Get(), y.Get());
  Revision::currentRevision()->Join(r);

  printf("after join: %d, %d\n", x.Get(), y.Get());

  return 0;
}
