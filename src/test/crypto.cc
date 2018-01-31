#include <errno.h>
#include <time.h>

#include "gtest/gtest.h"
#include "include/types.h"
#include "auth/Crypto.h"
#include "common/Clock.h"
#include "common/ceph_crypto.h"
#include "common/ceph_context.h"
#include "global/global_context.h"

class CryptoEnvironment: public ::testing::Environment {
public:
  void SetUp() override {
    ceph::crypto::init(g_ceph_context);
  }
};

TEST(AES, ValidateSecret) {
  auto h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES128);
  int l;

  for (l=0; l<16; l++) {
    bufferptr bp(l);
    int err;
    err = h->validate_secret(bp);
    EXPECT_EQ(-EINVAL, err);
  }

  for (l=16; l<50; l++) {
    bufferptr bp(l);
    int err;
    err = h->validate_secret(bp);
    EXPECT_EQ(0, err);
  }
}

TEST(AES, Encrypt) {
  auto h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES128);
  char secret_s[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  };
  bufferptr secret(buffer::create_static(sizeof(secret_s), (char*)secret_s));

  std::unique_ptr<ceph::crypto::KeyHandler> kh;
  ASSERT_NO_THROW(kh = h->get_key_handler(secret));

  unsigned char plaintext_s[] = {
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
    0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
    0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
  };
  constexpr size_t plaintext_len = sizeof(plaintext_s);

  unsigned char want_cipher_s[] = {
    0xb3, 0x8f, 0x5b, 0xc9, 0x35, 0x4c, 0xf8, 0xc6,
    0x13, 0x15, 0x66, 0x6f, 0x37, 0xd7, 0x79, 0x3a,
    0x3a, 0xfa, 0xcf, 0xa9, 0xcd, 0x8a, 0x4b, 0x3c,
    0x9d, 0xee, 0x09, 0x9d, 0x6f, 0xeb, 0x6f, 0x96,
    0xdb, 0xaa, 0x14, 0x20, 0xd0, 0x37, 0x94, 0xa7,
    0xcf, 0xa2, 0x96, 0x14, 0x1e, 0xdf, 0x62, 0x31,
  };
  const auto want_cipher = bufferlist::static_from_mem((char*)want_cipher_s,
                                                       sizeof(want_cipher_s));

  // test every combination with up to three buffer segments
  char *p1 = (char*)plaintext_s; // start of first segment
  for (unsigned l1 = 0; l1 <= plaintext_len; l1++) {
    char *p2 = p1 + l1; // start of second segment
    for (unsigned l2 = 0; l1 + l2 <= plaintext_len; l2++) {
      char *p3 = p2 + l2; // start of third segment
      unsigned l3 = plaintext_len - l2 - l1;

      bufferlist plaintext;
      if (l1) plaintext.append(buffer::create_static(l1, p1));
      if (l2) plaintext.append(buffer::create_static(l2, p2));
      if (l3) plaintext.append(buffer::create_static(l3, p3));

      bufferlist cipher;
      ASSERT_NO_THROW(kh->encrypt(plaintext, cipher));
      EXPECT_EQ(want_cipher, cipher);
    }
  }
}

TEST(AES, Decrypt) {
  auto h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES128);
  char secret_s[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  };
  bufferptr secret(buffer::create_static(sizeof(secret_s), (char*)secret_s));

  std::unique_ptr<ceph::crypto::KeyHandler> kh;
  ASSERT_NO_THROW(kh = h->get_key_handler(secret));

  unsigned char cipher_s[] = {
    0xb3, 0x8f, 0x5b, 0xc9, 0x35, 0x4c, 0xf8, 0xc6,
    0x13, 0x15, 0x66, 0x6f, 0x37, 0xd7, 0x79, 0x3a,
    0x3a, 0xfa, 0xcf, 0xa9, 0xcd, 0x8a, 0x4b, 0x3c,
    0x9d, 0xee, 0x09, 0x9d, 0x6f, 0xeb, 0x6f, 0x96,
    0xdb, 0xaa, 0x14, 0x20, 0xd0, 0x37, 0x94, 0xa7,
    0xcf, 0xa2, 0x96, 0x14, 0x1e, 0xdf, 0x62, 0x31,
  };
  constexpr size_t cipher_len = sizeof(cipher_s);

  unsigned char plaintext_s[] = {
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
    0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
    0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
  };
  const auto want_plaintext = bufferlist::static_from_mem((char*)plaintext_s,
                                                          sizeof(plaintext_s));

  // test every combination with up to three buffer segments
  char *p1 = (char*)cipher_s; // start of first segment
  for (unsigned l1 = 0; l1 <= cipher_len; l1++) {
    char *p2 = p1 + l1; // start of second segment
    for (unsigned l2 = 0; l1 + l2 <= cipher_len; l2++) {
      char *p3 = p2 + l2; // start of third segment
      unsigned l3 = cipher_len - l2 - l1;

      bufferlist cipher;
      if (l1) cipher.append(buffer::create_static(l1, p1));
      if (l2) cipher.append(buffer::create_static(l2, p2));
      if (l3) cipher.append(buffer::create_static(l3, p3));

      bufferlist plaintext;
      ASSERT_NO_THROW(kh->decrypt(cipher, plaintext));
      EXPECT_EQ(want_plaintext, plaintext);
    }
  }
}

TEST(AES, Loop) {
  ceph::crypto::Random random;

  bufferptr secret(16);
  random.get_bytes(secret.c_str(), secret.length());

  bufferptr orig_plaintext(256);
  random.get_bytes(orig_plaintext.c_str(), orig_plaintext.length());

  bufferlist plaintext;
  plaintext.append(orig_plaintext.c_str(), orig_plaintext.length());

  for (int i=0; i<10000; i++) {
    bufferlist cipher;
    {
      auto h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES128);

      std::unique_ptr<ceph::crypto::KeyHandler> kh;
      ASSERT_NO_THROW(kh = h->get_key_handler(secret));

      ASSERT_NO_THROW(kh->encrypt(plaintext, cipher));
    }
    plaintext.clear();

    {
      auto h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES128);

      std::unique_ptr<ceph::crypto::KeyHandler> kh;
      ASSERT_NO_THROW(kh = h->get_key_handler(secret));

      ASSERT_NO_THROW(kh->decrypt(cipher, plaintext));
    }
  }

  bufferlist orig;
  orig.append(orig_plaintext);
  ASSERT_EQ(orig, plaintext);
}

TEST(AES, LoopKey) {
  ceph::crypto::Random random;
  bufferptr k(16);
  random.get_bytes(k.c_str(), k.length());
  ceph::crypto::Key key(CEPH_CRYPTO_AES128, ceph_clock_now(), k);

  bufferlist data;
  bufferptr r(128);
  random.get_bytes(r.c_str(), r.length());
  data.append(r);

  utime_t start = ceph_clock_now();
  int n = 100000;

  for (int i=0; i<n; ++i) {
    bufferlist encoded;
    ASSERT_NO_THROW(key.encrypt(g_ceph_context, data, encoded));
  }

  utime_t end = ceph_clock_now();
  utime_t dur = end - start;
  cout << n << " encoded in " << dur << std::endl;
}
