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

#include <sstream>
#include "Crypto.h"
#ifdef USE_NSS
# include <nspr.h>
# include <nss.h>
# include <pk11pub.h>
#endif

#include "include/assert.h"
#include "common/Clock.h"
#include "common/armor.h"
#include "common/ceph_crypto.h"
#include "common/hex.h"
#include "common/safe_io.h"
#include "include/ceph_fs.h"
#include "include/compat.h"
#include "common/Formatter.h"
#include "common/debug.h"
#include <errno.h>

#ifdef HAVE_GETENTROPY
#include <unistd.h>
#endif

namespace ceph::crypto {

// use getentropy() if available. it uses the same source of randomness
// as /dev/urandom without the filesystem overhead
#ifdef HAVE_GETENTROPY

Random::Random() : fd(0) {}
Random::~Random() = default;

void Random::get_bytes(char *buf, int len)
{
  auto ret = TEMP_FAILURE_RETRY(::getentropy(buf, len));
  if (ret < 0) {
    throw std::system_error(errno, std::system_category());
  }
}

#else // !HAVE_GETENTROPY

// open /dev/urandom once on construction and reuse the fd for all reads
Random::Random()
  : fd(TEMP_FAILURE_RETRY(::open("/dev/urandom", O_RDONLY)))
{
  if (fd < 0) {
    throw std::system_error(errno, std::system_category());
  }
}

Random::~Random()
{
  VOID_TEMP_FAILURE_RETRY(::close(fd));
}

void Random::get_bytes(char *buf, int len)
{
  auto ret = safe_read_exact(fd, buf, len);
  if (ret < 0) {
    throw std::system_error(-ret, std::system_category());
  }
}

#endif


// CEPH_CRYPTO_NONE
namespace none {

class KeyHandlerImpl : public KeyHandler {
public:
  size_t block_size() const override { return 1; }
  void encrypt(const bufferlist& in, bufferlist& out) const override {
    out = in;
  }
  void decrypt(const bufferlist& in, bufferlist& out) const override {
    out = in;
  }
};

class HandlerImpl : public Handler {
public:
  int get_type() const override {
    return CEPH_CRYPTO_NONE;
  }
  int create(Random *random, bufferptr& secret) override {
    return 0;
  }
  int validate_secret(const bufferptr& secret) override {
    return 0;
  }
  std::unique_ptr<KeyHandler> get_key_handler(const bufferptr& secret) override {
    return std::make_unique<KeyHandlerImpl>();
  }
};

} // namespace none


// CEPH_CRYPTO_AES128
namespace aes128 {

static constexpr size_t BLOCK_SIZE = 16;

class HandlerImpl : public Handler {
public:
  int get_type() const override {
    return CEPH_CRYPTO_AES128;
  }
  int create(Random *random, bufferptr& secret) override;
  int validate_secret(const bufferptr& secret) override;
  std::unique_ptr<KeyHandler> get_key_handler(const bufferptr& secret) override;
};

#ifdef USE_NSS
// when we say AES, we mean AES-128
# define AES_KEY_LEN	16

class nss_exception : public std::exception {
  static constexpr size_t buffer_size = 128;
  char buffer[buffer_size];
 public:
  nss_exception(const char *operation, PRErrorCode code) noexcept {
    auto n = snprintf(buffer, buffer_size, "%s failed with %d", operation, code);
    buffer[n] = 0;
  }
  const char* what() const noexcept {
    return buffer;
  }
};

// unique_ptr wrappers
struct slotinfo_deleter {
  void operator()(PK11SlotInfo *slot) noexcept {
    PK11_FreeSlot(slot);
  }
};
using pk11_slotinfo_ptr = std::unique_ptr<PK11SlotInfo, slotinfo_deleter>;

struct symkey_deleter {
  void operator()(PK11SymKey *key) noexcept {
    PK11_FreeSymKey(key);
  }
};
using pk11_symkey_ptr = std::unique_ptr<PK11SymKey, symkey_deleter>;

struct context_deleter {
  void operator()(PK11Context *ctx) noexcept {
    PK11_DestroyContext(ctx, PR_TRUE);
  }
};
using pk11_context_ptr = std::unique_ptr<PK11Context, context_deleter>;

struct secitem_deleter {
  void operator()(SECItem *param) noexcept {
    SECITEM_FreeItem(param, PR_TRUE);
  }
};
using secitem_ptr = std::unique_ptr<SECItem, secitem_deleter>;

// cipher operations require the input buffers to be sized on block
// boundaries, though bufferlists may contain segments that are not
bool all_segments_aligned(const bufferlist& in, size_t block_size)
{
  for (auto& p : in.buffers()) {
    if (p.length() % block_size) {
      return false;
    }
  }
  return true;
}

template <typename Func> // Func(unsigned char*, size_t)
void nss_aes_op_aligned(const bufferlist& in, Func& cipher_op)
{
  // process each block-aligned buffer segment
  for (auto& p : in.buffers()) {
    cipher_op((unsigned char*)p.c_str(), p.length());
  }
}

template <size_t BlockSize, typename Func> // Func(unsigned char*, size_t)
void nss_aes_op_unaligned(const bufferlist& in, Func& cipher_op)
{
  // carry unaligned bytes between segments until we have a full block
  std::array<unsigned char, BlockSize> carry;
  auto carry_pos = carry.begin();
  auto carry_left = carry.size();

  // process each buffer segment
  for (auto& p : in.buffers()) {
    auto in_pos = (unsigned char*)p.c_str();
    auto in_len = p.length();

    if (carry_pos != carry.begin()) {
      const auto count = std::min<size_t>(carry_left, in_len);
      carry_pos = std::copy(in_pos, in_pos + count, carry_pos);
      carry_left -= count;
      if (carry_left == 0) {
        // process the carry block
        cipher_op(carry.data(), carry.size());
        carry_pos = carry.begin();
        carry_left = carry.size();
      }
      in_pos += count;
      in_len -= count;
    }
    // round down to block size
    const auto count = (in_len / BlockSize) * BlockSize;
    if (count > 0) {
      // process the aligned input buffer
      cipher_op(in_pos, count);
      in_pos += count;
      in_len -= count;
    }
    // carry any remaining bytes
    if (in_len > 0) {
      assert(in_len <= carry_left);
      carry_pos = std::copy(in_pos, in_pos + in_len, carry_pos);
      carry_left -= in_len;
    }
  }
  assert(carry_pos == carry.begin());
}

template <size_t BlockSize>
static void nss_aes_operation(CK_ATTRIBUTE_TYPE op,
                              CK_MECHANISM_TYPE mechanism,
                              PK11SymKey *key,
                              SECItem *param,
                              const bufferlist& in, bufferlist& out)
{
  if (in.length() % BlockSize) {
    throw std::invalid_argument("buffer length not a multiple of block size");
  }

  pk11_context_ptr ectx{PK11_CreateContextBySymKey(mechanism, op, key, param)};
  if (!ectx) {
    throw nss_exception("PK11_CreateContextBySymKey", PR_GetError());
  }

  // sample source said this has to be at least size of input + 8,
  // but i see 15 still fail with SEC_ERROR_OUTPUT_LEN
  bufferptr out_tmp(in.length()+16);
  auto out_pos = (unsigned char*)out_tmp.c_str();
  int out_left = out_tmp.length();

  unsigned out_len = 0;

  // lambda that processes a single block-aligned input buffer
  auto cipher_op = [&] (unsigned char* buf, size_t len) {
    int written = 0;
    auto ret = PK11_CipherOp(ectx.get(), out_pos, &written, out_left, buf, len);
    if (ret != SECSuccess) {
      throw nss_exception("PK11_CipherOp", PR_GetError());
    }
    // advance the output buffer position
    out_pos += written;
    out_len += written;
    out_left -= written;
  };

  if (all_segments_aligned(in, BlockSize)) {
    nss_aes_op_aligned(in, cipher_op);
  } else {
    nss_aes_op_unaligned<BlockSize>(in, cipher_op);
  }

  unsigned int written = 0;
  auto ret = PK11_DigestFinal(ectx.get(), out_pos, &written, out_left);
  if (ret != SECSuccess) {
    throw nss_exception("PK11_DigestFinal", PR_GetError());
  }
  out_len += written;

  out_tmp.set_length(out_len);
  out.append(out_tmp);
}

class KeyHandlerImpl : public KeyHandler {
  static constexpr CK_MECHANISM_TYPE mechanism = CKM_AES_CBC_PAD;
  pk11_slotinfo_ptr slot;
  pk11_symkey_ptr key;
  secitem_ptr param;
  bufferptr secret;

public:
  KeyHandlerImpl(pk11_slotinfo_ptr&& slot, pk11_symkey_ptr&& key,
                 secitem_ptr&& param, const bufferptr& secret)
    : slot(std::move(slot)),
      key(std::move(key)),
      param(std::move(param)),
      secret(secret)
  {}

  size_t block_size() const override { return BLOCK_SIZE; }

  void encrypt(const bufferlist& in, bufferlist& out) const override {
    nss_aes_operation<BLOCK_SIZE>(CKA_ENCRYPT, mechanism, key.get(),
                                  param.get(), in, out);
  }
  void decrypt(const bufferlist& in, bufferlist& out) const override {
    nss_aes_operation<BLOCK_SIZE>(CKA_DECRYPT, mechanism, key.get(),
                                  param.get(), in, out);
  }
};

std::unique_ptr<KeyHandler> HandlerImpl::get_key_handler(const bufferptr& secret)
{
  constexpr CK_MECHANISM_TYPE mechanism = CKM_AES_CBC_PAD;

  pk11_slotinfo_ptr slot{PK11_GetBestSlot(mechanism, nullptr)};
  if (!slot) {
    throw nss_exception("PK11_GetBestSlot", PR_GetError());
  }

  SECItem keyItem;
  keyItem.type = siBuffer;
  keyItem.data = (unsigned char*)secret.c_str();
  keyItem.len = secret.length();
  pk11_symkey_ptr key{PK11_ImportSymKey(slot.get(), mechanism,
                                        PK11_OriginUnwrap, CKA_ENCRYPT,
                                        &keyItem, nullptr)};
  if (!key) {
    throw nss_exception("PK11_ImportSymKey", PR_GetError());
  }

  SECItem ivItem;
  ivItem.type = siBuffer;
  // losing constness due to SECItem.data; IV should never be
  // modified, regardless
  ivItem.data = (unsigned char*)CEPH_AES_IV;
  ivItem.len = sizeof(CEPH_AES_IV);

  secitem_ptr param{PK11_ParamFromIV(mechanism, &ivItem)};
  if (!param) {
    throw nss_exception("PK11_ParamFromIV", PR_GetError());
  }

  return std::make_unique<KeyHandlerImpl>(std::move(slot), std::move(key),
                                          std::move(param), secret);
}

#else
# error "No supported crypto implementation found."
#endif



// ------------------------------------------------------------

int HandlerImpl::create(Random *random, bufferptr& secret)
{
  bufferptr buf(AES_KEY_LEN);
  random->get_bytes(buf.c_str(), buf.length());
  secret = std::move(buf);
  return 0;
}

int HandlerImpl::validate_secret(const bufferptr& secret)
{
  if (secret.length() < (size_t)AES_KEY_LEN) {
    return -EINVAL;
  }

  return 0;
}

} // namespace aes128


// ---------------------------------------------------


void Key::encode(bufferlist& bl) const
{
  using ceph::encode;
  encode(type, bl);
  encode(created, bl);
  __u16 len = secret.length();
  encode(len, bl);
  bl.append(secret);
}

void Key::decode(bufferlist::iterator& bl)
{
  using ceph::decode;
  decode(type, bl);
  decode(created, bl);
  __u16 len;
  decode(len, bl);
  bufferptr tmp;
  bl.copy_deep(len, tmp);
  if (_set_secret(type, tmp) < 0)
    throw buffer::malformed_input("malformed secret");
}

int Key::set_secret(int type, const bufferptr& s, utime_t c)
{
  int r = _set_secret(type, s);
  if (r < 0)
    return r;
  this->created = c;
  return 0;
}

int Key::_set_secret(int t, const bufferptr& s)
{
  if (s.length() == 0) {
    secret = s;
    ckh.reset();
    return 0;
  }

  auto ch = Handler::create(t);
  if (ch) {
    int ret = ch->validate_secret(s);
    if (ret < 0) {
      return ret;
    }
    try {
      ckh = ch->get_key_handler(s);
    } catch (const std::exception&) {
      return -EIO;
    }
  } else {
    return -EOPNOTSUPP;
  }
  type = t;
  secret = s;
  return 0;
}

int Key::create(CephContext *cct, int t)
{
  auto ch = Handler::create(t);
  if (!ch) {
    if (cct)
      lderr(cct) << "ERROR: cct->get_crypto_handler(type=" << t << ") returned NULL" << dendl;
    return -EOPNOTSUPP;
  }
  bufferptr s;
  int r = ch->create(cct->random(), s);
  if (r < 0)
    return r;

  r = _set_secret(t, s);
  if (r < 0)
    return r;
  created = ceph_clock_now();
  return r;
}

void Key::print(std::ostream &out) const
{
  out << encode_base64();
}

void Key::to_str(std::string& s) const
{
  int len = secret.length() * 4;
  char buf[len];
  hex2str(secret.c_str(), secret.length(), buf, len);
  s = buf;
}

void Key::encode_formatted(string label, Formatter *f, bufferlist &bl)
{
  f->open_object_section(label.c_str());
  f->dump_string("key", encode_base64());
  f->close_section();
  f->flush(bl);
}

void Key::encode_plaintext(bufferlist &bl)
{
  bl.append(encode_base64());
}


// ------------------

std::unique_ptr<Handler> Handler::create(int type)
{
  switch (type) {
  case CEPH_CRYPTO_NONE:
    return std::make_unique<none::HandlerImpl>();
  case CEPH_CRYPTO_AES128:
    return std::make_unique<aes128::HandlerImpl>();
  default:
    return NULL;
  }
}

} // namespace ceph::crypto
