# Quiche HTTP/3 Frontend

## Building

### Build BoringSSL as a static library

	~ $ git clone https://boringssl.googlesource.com/boringssl
	~ $ cd boringssl
	~/boringssl $ git checkout f1c75347daa2ea81a941e953f2263e0a4d970c8d
	~/boringssl $ cmake -GNinja -Bbuild -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON
	~/boringssl $ cmake --build build
	~/boringssl $ ls build/lib{crypto,ssl}.a

### Build Quiche against our BoringSSL instead of its submodule

	~ $ git clone http://github.com/cloudflare/quiche
	~ $ cd quiche
	~/quiche $ git checkout 0.16.0
	~/quiche $ QUICHE_BSSL_PATH=$HOME/boringssl cargo build --features ffi,pkg-config-meta,qlog --release
	~/quiche $ ls target/release/libquiche.a

### Build Curl against BoringSSL and Quiche

	~ $ git clone https://github.com/curl/curl
	~ $ cd curl
	~/curl $ git checkout curl-7_87_0
	~/curl $ LDFLAGS="-lm" cmake -GNinja -Bbuild -DCMAKE_BUILD_TYPE=Release -DHTTP_ONLY=ON -DOPENSSL_USE_STATIC_LIBS=ON -DOPENSSL_ROOT_DIR=$HOME/boringssl/build -DOPENSSL_INCLUDE_DIR=$HOME/boringssl/src/include -DUSE_QUICHE=ON -DQUICHE_LIBRARY=$HOME/quiche/target/release/libquiche.a -DQUICHE_INCLUDE_DIR=$HOME/quiche/quiche/include
	~/curl $ cmake --build build
	~/curl $ ls build/src/curl build/lib/libcurl.so

### Build Ceph with all of the above

	~/ceph/build $ cmake -GNinja -DCMAKE_BUILD_TYPE=Debug -DWITH_MGR=OFF -DWITH_RADOSGW_KMIP=OFF -DWITH_RADOSGW_DBSTORE=OFF -DWITH_RADOSGW_QUICHE=ON -DOPENSSL_USE_STATIC_LIBS=ON -DOPENSSL_ROOT_DIR=$HOME/boringssl/build -DOPENSSL_INCLUDE_DIR=$HOME/boringssl/src/include -DQUICHE_LIBRARY=$HOME/quiche/target/release/libquiche.a -DQUICHE_INCLUDE_DIR=$HOME/quiche/quiche/include -DCURL_LIBRARY=$HOME/curl/build/lib/libcurl.so -DCURL_INCLUDE_DIR=$HOME/curl/include ..
	~/ceph/build $ cmake --build . --target vstart

## Testing

### Create a self-signed test certificate

	~/ceph/build $ openssl req -new -newkey rsa:4096 -x509 -sha256 -days 365 -nodes -out rgw.crt -keyout rgw.key

### Start a test cluster with both frontends

	~/ceph/build $ OSD=1 MON=1 RGW=1 MGR=0 MDS=0 ../src/vstart.sh -n -d --rgw_frontend 'beast ssl_port=8000 ssl_certificate=rgw.crt ssl_private_key=rgw.key, quiche port=8000 cert=rgw.crt key=rgw.key'

### Create a testbucket

	~/ceph/build $ SSL_CERT_FILE=rgw.crt ~/curl/build/src/curl --http3 --aws-sigv4 aws:amz:us-east-1:s3 -u '0555b35654ad1656d804:h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==' -H x-amz-content-sha256:UNSIGNED-PAYLOAD https://localhost:8000/testbucket -X PUT -v

### Create, upload, and download an object

	dd if=/dev/zero of=128m.iso bs=1M count=128
	~/ceph/build $ SSL_CERT_FILE=rgw.crt ~/curl/build/src/curl --http3 --aws-sigv4 aws:amz:us-east-1:s3 -u '0555b35654ad1656d804:h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==' -H x-amz-content-sha256:UNSIGNED-PAYLOAD https://localhost:8000/testbucket/128m.iso -T 128m.iso -v
	~/ceph/build $ SSL_CERT_FILE=rgw.crt ~/curl/build/src/curl --http3 --aws-sigv4 aws:amz:us-east-1:s3 -u '0555b35654ad1656d804:h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==' -H x-amz-content-sha256:UNSIGNED-PAYLOAD https://localhost:8000/testbucket/128m.iso --output 128m.iso -v

For comparison against the beast frontend, remove the --http3 option.
