// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_AUTH_CRYPTO_H
#define CEPH_AUTH_CRYPTO_H

#include <memory>
#include <string>

#include "include/types.h"
#include "include/utime.h"
#include "include/buffer.h"

class CephContext;

namespace ceph {

class Formatter;

namespace crypto {

/*
 * Random byte stream generator suitable for cryptographic use
 */
class Random {
  const int fd;
 public:
  Random(); // throws on failure
  ~Random();

  /// copy up to 256 random bytes into the given buffer. throws on failure
  void get_bytes(char *buf, int len);
};

/*
 * some per-key context that is specific to a particular crypto backend
 */
class KeyHandler {
public:
  bufferptr secret;

  virtual ~KeyHandler() {}

  /// returns the required block size in bytes. the length of buffer segments
  /// passed as input to encrypt()/decrypt() must be a multiple of this size
  virtual size_t block_size() const = 0;

  virtual void encrypt(const bufferlist& in, bufferlist& out) const = 0;
  virtual void decrypt(const bufferlist& in, bufferlist& out) const = 0;
};

/*
 * match encoding of struct ceph_secret
 */
class Key {
protected:
  __u16 type;
  utime_t created;
  bufferptr secret;   // must set this via set_secret()!

  // cache a pointer to the implementation-specific key handler, so we
  // don't have to create it for every crypto operation.
  mutable ceph::shared_ptr<KeyHandler> ckh;

  int _set_secret(int type, const bufferptr& s);

public:
  Key() : type(0) { }
  Key(int t, utime_t c, bufferptr& s)
    : created(c) {
    _set_secret(t, s);
  }
  ~Key() {
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);

  int get_type() const { return type; }
  utime_t get_created() const { return created; }
  void print(std::ostream& out) const;

  int set_secret(int type, const bufferptr& s, utime_t created);
  const bufferptr& get_secret() { return secret; }
  const bufferptr& get_secret() const { return secret; }

  void encode_base64(string& s) const {
    bufferlist bl;
    encode(bl);
    bufferlist e;
    bl.encode_base64(e);
    e.append('\0');
    s = e.c_str();
  }
  string encode_base64() const {
    string s;
    encode_base64(s);
    return s;
  }
  void decode_base64(const string& s) {
    bufferlist e;
    e.append(s);
    bufferlist bl;
    bl.decode_base64(e);
    bufferlist::iterator p = bl.begin();
    decode(p);
  }

  void encode_formatted(string label, Formatter *f, bufferlist &bl);
  void encode_plaintext(bufferlist &bl);

  // --
  int create(CephContext *cct, int type);
  void encrypt(CephContext *cct, const bufferlist& in, bufferlist& out) const {
    assert(ckh); // Bad key?
    ckh->encrypt(in, out);
  }
  void decrypt(CephContext *cct, const bufferlist& in, bufferlist& out) const {
    assert(ckh); // Bad key?
    ckh->decrypt(in, out);
  }

  void to_str(std::string& s) const;
};
WRITE_CLASS_ENCODER(Key)

static inline std::ostream& operator<<(std::ostream& out, const Key& k)
{
  k.print(out);
  return out;
}


/*
 * Driver for a particular algorithm
 *
 * To use these functions, you need to call ceph::crypto::init(), see
 * common/ceph_crypto.h. common_init_finish does this for you.
 */
class Handler {
public:
  virtual ~Handler() {}
  virtual int get_type() const = 0;
  virtual int create(Random *random, bufferptr& secret) = 0;
  virtual int validate_secret(const bufferptr& secret) = 0;
  virtual std::unique_ptr<KeyHandler> get_key_handler(const bufferptr& secret) = 0;

  static std::unique_ptr<Handler> create(int type);
};

} // namespace crypto
} // namespace ceph

#endif
