#include <set>
#include <map>
#include <pthread.h>

class Revision;
class Segment;
class IAction;

class IAction {
 public:
  virtual void Do() = 0;
};

class IVersioned {
 public:
  virtual void Release(Segment *release) = 0;
  virtual void Collapse(Revision *main, Segment *parent) = 0;
  virtual void Merge(Revision *main, Revision *joinRev, Segment *join) = 0;
};

template <typename T>
class Versioned : private IVersioned {

  std::map<int, T> versions;

 public:
  Versioned();
  explicit Versioned(const T& v);

  T Get();
  void Set(T value);

 private:
  T Get(Revision *r);
  void Set(Revision *r, T value);

  void Release(Segment *segment);
  void Collapse(Revision *main, Segment *parent);
  void Merge(Revision *main, Revision *joinee, Segment *join);

  Versioned(const Versioned&);
};

class Revision {
 public:
  Segment *const root;
  Segment *current;
 private:
  pthread_t task;
  static pthread_key_t key; // thread local storage
  static pthread_once_t once; // once initializer

 public:

  ~Revision();

  Revision *Fork(IAction *action);
  void Join(Revision *join);

  static Revision* currentRevision();

 private:
  Revision();

  Revision(Segment *root, Segment *current)
    : root(root), current(current) {
  }

  static Revision* setCurrentRevision(Revision *r) {
    printf("(%08lx) revision %10p -> %10p\n",
	   pthread_self(), pthread_getspecific(key), r);
    return 0 == pthread_setspecific(key, r) ? r : 0;
  }

  static void create_tls() {
    if (0 == pthread_key_create(&key, delete_tls)) {
      static struct destruct {
	~destruct() {
	  //TODO: resouce leak
	  //delete_tls(pthread_getspecific(key));
	  pthread_key_delete(key);
	}
      } destruct;
    }
  }

  static void delete_tls(void *data) {
    //TODO: resource leak
    //delete static_cast<Revision*>(data);
    //pthread_setspecific(key, 0);
  }

  class RunContext;
  static void* run(void *);
};

class Segment {
 public:
  Segment *parent;
 private:
  int refcount;
 public:
  std::set<IVersioned*> written;
 private:
  static int versionCount;

 public:
  int version;

  explicit Segment(Segment *parent)
    : parent(parent), version(versionCount++), refcount(1) {
    if (parent) ++parent->refcount;
  }

  void Release() {
    if (0 == --refcount) {
      std::set<IVersioned*>::iterator it;
      for (it = written.begin(); it != written.end(); ++it) {
        (*it)->Release(this);
      }
      if (parent) parent->Release();
    }
  }

  void Collapse(Revision *main) {
    // assert: main->current == this
    while (parent != main->root && parent->refcount == 1) {
      std::set<IVersioned*>::iterator it;
      for (it = written.begin(); it != written.end(); ++it) {
        (*it)->Collapse(main, parent);
      }
      parent = parent->parent;
    }
  }
};

pthread_key_t Revision::key; // thread local storage
pthread_once_t Revision::once = PTHREAD_ONCE_INIT;
int Segment::versionCount = 0;

template <typename T>
Versioned<T>::Versioned() { Set(T()); }

template <typename T>
Versioned<T>::Versioned(const T& v) { Set(v); }

template <typename T>
T Versioned<T>::Get() { return Get(Revision::currentRevision()); }

template <typename T>
void Versioned<T>::Set(T value) { Set(Revision::currentRevision(), value); }

template <typename T>
T Versioned<T>::Get(Revision *r) {
  Segment *s = r->current;
  printf("Get version %d of %p\n", s->version, this);
  while (versions.find(s->version) == versions.end()) {
    printf("  search parent of version %d\n", s->version);
    s = s->parent;
  }
  return versions[s->version];
}

template <typename T>
void Versioned<T>::Set(Revision *r, T value) {
  if (versions.find(r->current->version) == versions.end()) {
printf("insert version %d\n", r->current->version);
    r->current->written.insert(this);
  }
  versions[r->current->version] = value;
}

template <typename T>
void Versioned<T>::Release(Segment *release) {
printf("erasing version %d\n", release->version);
  versions.erase(release->version);
}

template <typename T>
void Versioned<T>::Collapse(Revision *main, Segment *parent) {
  if (versions.find(main->current->version) == versions.end()) {
    Set(main, versions[parent->version]);
  }
  versions.erase(parent->version);
}

template <typename T>
void Versioned<T>::Merge(Revision *main, Revision *joinee, Segment *join) {
  Segment *s = joinee->current;
  while (versions.find(s->version) == versions.end()) {
    s = s->parent;
  }
  if (s == join) { // only merge if this was the last write
    Set(main, versions[join->version]);
  }
}

Revision::~Revision() {
  delete current;
  printf("deleting revision %10p\n", this);
}

class Revision::RunContext {
 public:
  IAction *const action;
  Revision *const prev;
  Revision *const next;
  RunContext(IAction *action, Revision *prev, Revision *next)
    : action(action), prev(prev), next(next) {}
};

Revision *Revision::Fork(IAction *action) {
  Revision *r = new Revision(current, new Segment(current));
  current->Release();
  current = new Segment(current);
  pthread_create(&task, NULL, run,
		 new RunContext(action, currentRevision(), r));
  return r;
}

void Revision::Join(Revision *join) {
  pthread_join(task, NULL);
  Segment *s = join->current;
  while (s != join->root) {
    std::set<IVersioned*>::iterator it;
    for (it = s->written.begin(); it != s->written.end(); ++it) {
      (*it)->Merge(this, join, s);
    }
    s = s->parent;
  }
  join->current->Release();
  current->Collapse(this);
}

Revision* Revision::currentRevision() {
  pthread_once(&once, create_tls); // prepare TLS if not initialized yet
  void *const revision = pthread_getspecific(key);
  // in the IAction::Do, revision is set in its thread function.
  // but main thread, there is no revision so create and set it.
  return revision ? static_cast<Revision*>(revision)
    : setCurrentRevision(new Revision(0, new Segment(0)));
}

void* Revision::run(void *context) {
  RunContext ctx = *static_cast<RunContext*>(context); // copy
  delete static_cast<RunContext*>(context); // release buffer on heap
  pthread_setspecific(key, ctx.prev);
  setCurrentRevision(ctx.next);
  ctx.action->Do();
  //setCurrentRevision(ctx.prev);
  return 0;
}
