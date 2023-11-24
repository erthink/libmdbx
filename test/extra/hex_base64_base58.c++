#include "mdbx.h++"
#include <array>
#include <iostream>
#include <unistd.h>

#include <functional>
#include <random>

using buffer = mdbx::default_buffer;

std::default_random_engine prng(42);

static buffer random(size_t length) {
  buffer result(length);
#if defined(__cpp_lib_span) && __cpp_lib_span >= 202002L
  for (auto &i : result.bytes())
    i = prng();
#else
  for (auto p = result.byte_ptr(); p < result.end_byte_ptr(); ++p)
    *p = mdbx::byte(prng());
#endif
  return result;
}

static bool basic() {
  bool ok = true;
  const char *const hex_dump = "1D58fa\n2e46E3\nBd9c7A\nC0bF";
  const uint8_t native[] = {0x1D, 0x58, 0xfa, 0x2e, 0x46, 0xE3,
                            0xBd, 0x9c, 0x7A, 0xC0, 0xbF};

  if (mdbx::slice(hex_dump).hex_decode(true) != mdbx::slice::wrap(native))
    std::cerr << "hex_decode() failed\n";
  else if (mdbx::slice::wrap(native).encode_hex(true, 4).hex_decode(true) !=
           mdbx::slice::wrap(native))
    std::cerr << "hex_encode(UPPERCASE) failed\n";
  else if (mdbx::slice::wrap(native).encode_hex(false).hex_decode(true) !=
           mdbx::slice::wrap(native))
    std::cerr << "hex_encode(lowercase) failed\n";

  if (mdbx::slice("").as_base64_string() != "" ||
      mdbx::slice(" ").encode_base64().as_string() != "IA==" ||
      mdbx::slice("~0").encode_base64().as_string() != "fjA=" ||
      mdbx::slice("A_z").encode_base64().as_string() != "QV96" ||
      mdbx::slice("Ka9q").encode_base64().as_string() != "S2E5cQ==" ||
      mdbx::slice("123456789").encode_base64().as_string() != "MTIzNDU2Nzg5") {
    std::cerr << "encode_base64() failed\n";
    ok = false;
  }

  const uint8_t base58_rfc[] = {0x00, 0x00, 0x28, 0x7f, 0xb4, 0xcd};
  if (mdbx::slice("").as_base58_string() != "" ||
      mdbx::slice(" ").encode_base58().as_string() != "Z" ||
      mdbx::slice("Hello World!").as_base58_string() != "2NEpo7TZRRrLZSi2U" ||
      mdbx::slice("The quick brown fox jumps over the lazy dog.")
              .encode_base58()
              .as_string() !=
          "USm3fpXnKG5EUBx2ndxBDMPVciP5hGey2Jh4NDv6gmeo1LkMeiKrLJUUBk6Z" ||
      mdbx::slice::wrap(base58_rfc).as_base58_string() != "11233QC4" ||
      mdbx::slice("~0").encode_base58().as_string() != "Aby" ||
      mdbx::slice("A_z").encode_base58().as_string() != "NxZw" ||
      mdbx::slice("Ka9q").encode_base58().as_string() != "2vkjDi" ||
      mdbx::slice("123456789").encode_base58().as_string() != "dKYWwnRHc7Ck") {
    std::cerr << "encode_base58() failed\n";
    ok = false;
  }

  if (mdbx::slice("").base58_decode() != mdbx::slice() ||
      mdbx::slice("Z").base58_decode() != mdbx::slice(" ") ||
      mdbx::slice("2NEpo7TZRRrLZSi2U").base58_decode() != "Hello World!" ||
      mdbx::slice(
          "USm3fpXnKG5EUBx2ndxBDMPVciP5hGey2Jh4NDv6gmeo1LkMeiKrLJUUBk6Z")
              .base58_decode() !=
          mdbx::slice("The quick brown fox jumps over the lazy dog.") ||
      mdbx::slice("11233QC4").base58_decode() !=
          mdbx::slice::wrap(base58_rfc) ||
      mdbx::slice("Aby").base58_decode() != mdbx::slice("~0") ||
      mdbx::slice("NxZw").base58_decode() != mdbx::slice("A_z") ||
      mdbx::slice("2vkjDi").base58_decode() != mdbx::slice("Ka9q") ||
      mdbx::slice("dKYWwnRHc7Ck").base58_decode() != mdbx::slice("123456789")) {
    std::cerr << "decode_base58() failed\n";
    ok = false;
  }

  return ok;
}

int main(int argc, const char *argv[]) {
  (void)argc;
  (void)argv;

  auto ok = basic();
  for (size_t n = 0; n < 1000; ++n) {
    for (size_t length = 0; ok && length < 111; ++length) {
      const auto pattern = random(length);
      if (pattern != pattern.encode_hex(bool(prng() & 1), prng() % 111)
                         .hex_decode(true)
                         .encode_hex()
                         .hex_decode(false)) {
        std::cerr << "hex encode/decode failed: n " << n << ", length "
                  << length << std::endl;
        ok = false;
      }
      if (pattern != pattern.encode_base64(unsigned(prng() % 111))
                         .base64_decode(true)
                         .encode_base64()
                         .base64_decode(false)) {
        std::cerr << "base64 encode/decode failed: n " << n << ", length "
                  << length << std::endl;
        ok = false;
      }
      if (pattern != pattern.encode_base58(unsigned(prng() % 111))
                         .base58_decode(true)
                         .encode_base58()
                         .base58_decode(false)) {
        std::cerr << "base58 encode/decode failed: n " << n << ", length "
                  << length << std::endl;
        ok = false;
      }
    }
  }

  if (!ok) {
    std::cerr << "Fail\n";
    return EXIT_FAILURE;
  }
  std::cout << "OK\n";
  return EXIT_SUCCESS;
}
